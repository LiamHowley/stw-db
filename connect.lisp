(in-package stw.db)


(defun parse-params (stream)
  (loop for line = (read-line stream nil)
     while line
     for (param value) = (explode-string line '(#\= #\") :remove-separators t)
     unless (or (char= (char line 0) #\#)
		(eql (length value) 0))
     collecting (intern (string-upcase param) 'keyword) into params and
     collecting value into params
     finally (return
	       (ordered-plist-values params :database :user :password :host :port :use-ssl :service))))

(define-layered-function connection-params (file)
  (:documentation "Return ordered list of parameter values from file.")

  (:method
      :in db-layer ((file pathname))
      (with-open-file (in file)
	(parse-params in)))

  (:method
      :in db-layer ((file string))
      (connection-params (pathname file)))

  (:method ((file pathname))
    (error "Database params cannot be accessed from this layer")))


(defvar *db*)

(defvar *pool-lock*)

(defvar *connections-limit* -1
  "A negative value indicates no limit and 0 a deactivated pool. 
NOTE: This limit applies to each defined database environment.
I.e. a value of 10 with ten defined database environments means
100 connections.")

(defdynamic db-params nil)

(defdynamic db-connection-pool nil)

(defdynamic db-pool-lock nil)


(defstruct (connection-pool (:conc-name nil))
  (pool nil :type list))

(defmacro define-db-environment (env params)
  "Define database environment for specific parameters.
Subsequent connections are established using DB-CONNECT 
macro, or it's wrappers DB-INTERFACE and DB-TABLE."
  `(defdynamic ,env
     (with-active-layers (db-layer)
       (dlet ((db-params ,params)
	      (db-connection-pool (make-connection-pool))
	      (db-pool-lock (make-lock "connection-pool-lock")))
	 (capture-dynamic-environment)))))

(defmacro db-connect (env (&optional (layer db-interface-layer)) &body body)
  "DB-CONNECT requires an environment, defined by the DEFINE-DB-ENVIRONMENT
macro, and a layer of type DB-LAYER, DB-INTERFACE-LAYER or DB-TABLE-LAYER.
Environment parameters are accessed within DB-CONNECT and the resulting connection
*DB* is made available for use in the BODY form."
  (with-gensyms (pool)
    `(with-context ,env 
       (with-active-layers (,layer)
	 (let* ((,pool (dynamic db-connection-pool))
		(*pool-lock* (dynamic db-pool-lock))
		(*db* (make-connection (dynamic db-params) ,pool)))
	   (when (and *db* (database-open-p *db*))
	     (unwind-protect (progn ,@body)
	       (close-connection *db* ,pool))))))))

(defmacro db-update-node (env node &body body)
  "Wrapper for DB-CONNECT using the DB-UPDATE-NODE-LAYER."
  `(db-connect ,env (update-node)
     (let ((original (clone-object ,node)))
       ,@body)))

(defmacro db-update-table (env node &body body)
  "Wrapper for DB-CONNECT using the DB-UPDATE-NODE-LAYER."
  `(db-connect ,env (update-table)
     (let ((original (clone-object ,node)))
       ,@body)))


;;(define-db-environment db
;;    (connection-params #P "~/.commune_db.params"))


(defmacro clear-connection-pool (env)
  "Closes all connections and removes them from 
the connection pool associated with env."
  `(with-dynamic-environment ((dynamic ,env))
     (loop
       with pool = (dynamic db-connection-pool)
       while pool
       do (close-database (pop pool)))))


(define-layered-function make-connection (params connection-pool)

  (:method (params connection-pool)
    (declare (ignore params connection-pool))
    (error "Database cannot be accessed from this layer"))

  (:method
      :in db-layer ((params list) connection-pool)
    (declare (ignore connection-pool))
    (apply #'open-database params))

  (:method
      :in db-layer ((params list) (pool connection-pool))
      (if (pool pool)
	  (handler-case 
	      (atomic-pop (pool pool))
	    (implementation-not-supported ()
	      (with-lock-held (*pool-lock*)
		(pop (pool pool)))))
	  (apply #'open-database params))))



(define-layered-function close-connection (connection connection-pool)
  (:documentation "When a connection-pool is passed,the connection 
is pushed to the pool. Otherwise the connection is closed.")

  (:method (connection connection-pool)
    (declare (ignore connection connection-pool))
    (error "Database cannot be accessed from this layer"))

  (:method
      :in db-layer ((connection database-connection) connection-pool)
    (declare (ignore connection-pool))
    (close-database connection))

  (:method
      :in db-layer ((connection database-connection) (pool connection-pool))
    (handler-case 
	(cond ((< *connections-limit* 0)
	       (atomic-push connection (pool pool)))
	      ((and (< (length (pool pool)) *connections-limit*))
	       (atomic-push connection (pool pool)))
	      (t
	       (close-database connection)))
      (implementation-not-supported () 
	(with-lock-held (*pool-lock*)
	  (push connection (pool pool)))))))

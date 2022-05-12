(in-package stw.db)


(define-layered-function (setf proc-template) (new-value class component &rest rest &key &allow-other-keys)

  (:method
      :in-layer db-op ((new-value procedure) (class serialize) component &rest rest &key &allow-other-keys)
    (let ((key (apply #'get-key class component rest)))
      (setf (gethash key (db-template-register)) new-value))))

(define-layered-function proc-template (class component &rest rest &key &allow-other-keys)

  (:method
      :in db-layer :around ((class serialize) component &rest rest &key &allow-other-keys)
    (multiple-value-bind (procedure existsp)
	(call-next-method)
      (when (and existsp procedure)
	procedure)))

  (:method
      :in-layer db-op ((class serialize) component &rest rest &key &allow-other-keys)
    (gethash (apply #'get-key class component rest) (db-template-register))))



(define-layered-function get-key (class component &rest rest &key &allow-other-keys))

(define-layered-method get-key
  :in db-op ((class serialize) component &rest rest &key)
  (nth-value 1 (slots-with-values class)))

(define-layered-method get-key
  :in db-op ((class serialize) (component db-table-class) &rest rest &key)
  (class-name component))



(define-layered-function db-template-register ()
  (:method
      :in-layer insert ()
    (template-register (contextl:find-layer 'insert)))
  (:method
      :in-layer delete-from ()
    (template-register (contextl:find-layer 'delete-from)))
  (:method
      :in-layer update ()
    (template-register (contextl:find-layer 'update)))
  (:method
      :in-layer retrieve ()
    (template-register (contextl:find-layer 'retrieve))))


(define-layered-function dispatch-statement (class procedure)
  (:method
      :in db-layer ((class serialize) (procedure procedure))
    (with-slots (p-control relevant-slots) procedure
      (apply #'format nil p-control
	     (nreverse
	      (labels ((walk (inner &optional acc parenthesize)
			 (cond ((null inner)
				acc)
			       ((atom inner)
				(let ((result (prepare-value inner (slot-value class (slot-definition-name inner)) parenthesize)))
				  (cond (result
					 (cons result acc))
					(t acc))))
			       (t (walk (cdr inner) (walk (car inner) acc (parenthesize inner)))))))
		(walk relevant-slots)))))))


(define-layered-function parenthesize (slots)
  (:method
      :in db-layer ((slots cons))
    (unless (> (length slots) 1)
      t)))


(define-layered-function read-row-to-class (class)

  (:method
      :in db-layer ((class serialize))
    (flet ((get-symbol-name (field)
	     (string-upcase (substitute #\- #\_ (string-left-trim '(#\_) field)))))
      (row-reader (fields)
	(let ((base-class (class-of class)))
	  (loop
	    while (next-row)
	    do (loop
		 for field across fields
		 for symbol-name = (get-symbol-name (field-name field))
		 for slot-name = (intern symbol-name (symbol-package (class-name base-class)))
		 when (find-slot-definition base-class slot-name 'db-column-slot-definition)
		   do (setf (slot-value class slot-name) (next-field field)))))))))


(define-layered-function execute (class component)

  (:method
      :in-layer db-layer
      :around ((class serialize) component)
    (multiple-value-bind (statement procedure)
	(call-next-method)
      (handler-case (exec-query *db* statement (read-row-to-class class))
	(database-error (err)
	  (error-handler class procedure err component))))
    class)

  (:method
      :in-layer db-layer ((class serialize) component)
    (values (or (proc-template class component)
		(setf (proc-template class component) (generate-procedure class component)))
	    (dispatch-statement class procedure))))


(define-layered-function error-handler (class procedure error component)

  (:method
      :in db-layer ((class serialize) (procedure procedure) (err database-error) component)
    (let* ((base-class (class-of class))
	   (schema (slot-value base-class 'schema))
	   (class-name (db-syntax-prep (class-name base-class))))
      (labels ((exec (statement)
		 (exec-query *db* statement))
	       (respond (proc-name &rest functions)
		 (multiple-value-bind (statement procedure)
		     (apply #'build-db-component
			    (if component component base-class)
			    proc-name functions)
		   (exec statement)
		   (exec (call-statement procedure)))
		 (execute class component)))
	;; in theory at least this should be safe from infinitely recursive
	;; loops as the dispatch statement and procedure are both generated
	;; by reference to the same object.
	(scase (database-error-code err)
	       ("3F000"
		;; MISSING SCHEMA
		;; Response: Create schema and recurse
		(exec (create-schema schema))
		(exec (set-schema schema))
		(execute class component))
	       ("42704"
		;; MISSING TYPE
		;; Response: Create types for all relevant
		;; table inserts. 
		(let ((proc-name (format nil "initialize_~(~a~)_types" class-name)))
		  (respond proc-name #'create-pg-composite #'create-typed-domain)))
	       ("42P01"
		;; MISSING TABLE OR TYPE IN DB.
		;; Response: rebuild database component and
		;; (re)initialize database.
		;; Note: An effective way to build a database is to
		;; let an insert fail and thus invoke this response.
		(let ((proc-name (format nil "initialize_~(~a~)_relations" class-name)))
		  (respond proc-name #'create-statement #'foreign-keys-statements #'index-statement)))
	       ("42883"
		;; MISSING INSERT PROCEDURE
		;; Response: Make procedure and recurse.
		(exec (sql-statement procedure))
		(execute class component))
	       (t (error err)))))))



(define-layered-function match-mapping-node (class table)

  (:documentation "Confirm class maps table, and return relevant instance
of SLOT-MAPPING.")

  (:method
    :in-layer db-layer ((class db-interface-class) table)
    (loop
      for mapping in (slot-value table 'mapped-by)
      when (eq (mapping-node mapping) class)
	do (return mapping))))


(define-layered-function prepare-value (slot value &optional parenthesize)
  (:method
      :in db-layer ((slot db-column-slot-definition) value &optional parenthesize)
    (with-slots (col-type default not-null) slot
      (cond (value
	     (case col-type
	       (:boolean
		(if (eq value t) "'t'" "'f'"))
	       ((:text :varchar)
		(if parenthesize
		    (concatenate 'string "'(" value ")'")
		    (concatenate 'string "'" value "'")))
	       (t
		value)))
	    ((and not-null default (eq col-type :boolean))
	     "'f'")
	    (default
	     "null"))))

  (:method
      :in db-layer ((slot db-aggregate-slot-definition) values &optional parenthesize)
    (with-slots (maps) slot
      (loop
	with column = (mapped-column maps)
	for value in values
	collect (prepare-value% column value parenthesize)))))



(define-layered-function prepare-value (slot col-type value parenthesize)

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :boolean)) value parenthesize)
    (declare (ignore slot col-type parenthesize))
    (if (eq value t) "'t'" "'f'"))

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :text)) (value string) parenthesize)
    (if parenthesize
	(concatenate 'string "'(" value ")'")
	(concatenate 'string "'" value "'")))

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :varchar)) (value string) parenthesize)
    (prepare-value slot :text value parenthesize))

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :char)) (value string) parenthesize)
    (prepare-value slot :text value parenthesize))


  ;;; numeric
  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :integer)) (value string) parenthesize)
    (declare (ignore slot col-type parenthesize))
    (parse-integer value))

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :integer)) (value float) parenthesize)
    (declare (ignore slot col-type parenthesize))
    (round value))

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :float)) (value integer) parenthesize)
    (declare (ignore slot col-type parenthesize))
    (float value))

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :float)) (value string) parenthesize)
    (declare (ignore slot col-type parenthesize))
    (float value))


  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :array)) (value cons) parenthesize)
    (declare (ignore slot col-type parenthesize))
    (format nil "'{~{~s~^, ~}}'" value))

  (:method
      :in db-layer ((slot db-column-slot-definition) (col-type (eql :array)) (value array) parenthesize)
    (prepare-value slot :array (array-to-list value) parenthesize))


  ;;; the rest
  (:method
      :in db-layer ((slot db-column-slot-definition) col-type value parenthesize)
    (declare (ignore col-type parenthesize))
    (cond (value
	   value)
	  ((slot-valude slot 'default)
	   "null"))))

(in-package stw.db)


(define-layered-function statement (class))

(define-layered-function clause (class))

;;; schema


(define-layered-function create-schema (schema)
  (:method
      :in db-layer ((schema schema))
    (with-slots (schema) schema
      (princ (format nil "Creating schema: ~a~%" schema))
      (format nil "CREATE SCHEMA IF NOT EXISTS ~(~a~)" schema))))


(define-layered-function set-schema (schema)
  (:method
      :in db-layer ((schema schema))
    (with-slots (schema) schema
      (princ (format nil "Search path set to: ~a~%" schema))
      (format nil "SET search_path TO ~(~a~), public" schema))))


(define-layered-function set-privileged-user (schema user)
  (:method
      :in db-layer ((schema schema) (user string))
    (with-slots (schema) schema
      (format nil "GRANT ALL PRIVILEGES ON SCHEMA ~(~a~) TO ~a" schema user))))


(define-layered-function initialize-schema (schema)
  (:method
      :in db-interface-layer ((schema schema))
    (labels ((exec (statement)
               (exec-query *db* statement))
             (builder (node name &rest functions)
               (multiple-value-bind (statement procedure)
                   (apply #'build-db-component node name functions)
                 (exec statement)
                 (exec (slot-value procedure 'p-control)))))
      (exec (create-schema schema))
      (exec (set-schema schema))
      (with-slots (nodes) schema
        (iterate
         (for node in nodes)
         (builder node (format nil "initialize_~(~a~)_types" (db-syntax-prep (class-name node)))
                  #'create-enumerated-types-statement #'create-pg-composite #'create-typed-domain)
         (builder node (format nil "initialize_~(~a~)_relations" (db-syntax-prep (class-name node)))
                  #'create-table-statement))
        (iterate
         (for node in nodes)
         (builder node (format nil "initialize_~(~a~)_keys" (db-syntax-prep (class-name node)))
                  #'foreign-keys-statements #'index-statement))))))


(defun funcall-tables (tables method)
  (flatten
   (loop
     for table in tables
     for object = (find-class table)
     for statement = (with-active-layers (db-table-layer)
                       (funcall method object))
     when statement
       collect statement)))


;;; create enumerated type

(define-layered-function create-enumerated-types-statement (class)
  (:documentation "Create enumerated-types in schema.")

  (:method
      :in db-interface-layer ((class db-interface-class))
    (funcall-tables (slot-value class 'tables) #'create-enumerated-types-statement))

  (:method
      :in db-table-layer ((class db-table-class))
    (with-slots (schema) class
      (loop
        for slot in (filter-slots-by-type class 'enumerated-column-slot-definition)
        unless (slot-value slot 'store-index)
          do (princ (format nil "Creating enumerated type: enum_~a in schema: ~s~%" (column-name slot) schema))
          and collect (statement slot)))))


(define-layered-method statement
  :in-layer db-table-layer ((statement enumerated-column-slot-definition))
  (with-slots (schema column-name enumerated) statement
    (let ((enum-type (set-sql-name column-name)))
      (format nil
              "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'enum_~a') THEN CREATE TYPE enum_~a AS ENUM (~{'~a'~^, ~}); END IF;"
              enum-type enum-type (array-to-list enumerated)))))



;;; create table

(define-layered-function create-table-statement (class)
  (:documentation "Create table(s) in schema.")

  (:method
      :in db-interface-layer ((class db-interface-class))
    (funcall-tables (slot-value class 'tables) #'create-table-statement))

  (:method
      :in db-table-layer ((class db-table-class))
    (with-slots (schema table primary-keys constraints) class
      (princ (format nil "Creating table: ~s in schema: ~s~%" table schema))
      (format nil "CREATE TABLE IF NOT EXISTS ~a.~a (~{~a~^, ~}~@[, ~a~]~@[, ~{~a~^, ~}~]);"
	            schema table
	            (loop for column in (filter-slots-by-type class 'db-column-slot-definition)
		                collect (clause column))
	            ;; primary keys
	            (clause (make-instance 'primary-key :keys primary-keys))
	            ;;check constraints
	            (loop for constraint in constraints
		                collect (clause (apply #'make-instance 'check-constraint :table table constraint)))))))


(define-layered-class primary-key
  :in-layer db-layer ()
  ((keys :initarg :keys :initform nil :reader keys)))


(define-layered-class check-constraint
  :in-layer db-layer ()
  ((table :initarg :table)
   (col-name :initarg :col-name)
   (check :initarg :check)
   constraint-name))


(defmethod initialize-instance :after ((this check-constraint) &key)
  (with-slots (check col-name table constraint-name) this
    (setf check (infix-constraint check col-name)
	        constraint-name (format nil "~a_~a_check" (db-syntax-prep table) (db-syntax-prep col-name)))))


(define-layered-function column-type (column)

  (:method 
      :in-layer db-table-layer ((column enumerated-column-slot-definition))
    (with-slots (store-index col-type column-name) column
      (if store-index col-type (format nil "enum_~a" column-name))))

  (:method
      :in-layer db-table-layer ((column date/time-column-slot-definition))
    (get-column-type column))

  (:method
      :in-layer db-table-layer ((column db-column-slot-definition))
    (slot-value column 'col-type)))


(define-layered-method clause
  :in-layer db-table-layer ((column db-column-slot-definition))
  (with-slots (column-name not-null col-type unique) column
    (let ((column-type (column-type column)))
      (format nil "~(~a~) ~{~a~}"
              column-name
              (list
               column-type
               (if not-null " NOT NULL" "")
               (if (eq unique t) " UNIQUE" "")
               (if (slot-boundp column 'default)
                   (let ((default (slot-value column 'default)))
                     (typecase default
                       (cons
                        (format nil " DEFAULT ~a(~@[~{~a~^, ~}~])" (car default) (cdr default)))
                       (integer
                        (format nil " DEFAULT ~a" default))
                       (string
                        (format nil " DEFAULT '~a'" default))
                       (boolean
                        (if (eq col-type :boolean)
                            (format nil " DEFAULT '~a'" (if (eq default t) "t" "f"))
                            ""))
                       (t "")))
                   ""))))))


(define-layered-method clause
  :in-layer db-table-layer ((clause primary-key))
  (with-slots (keys) clause
    (when keys
      (format nil "PRIMARY KEY (~{~a~^, ~})" (mapcar #'column-name keys)))))


(define-layered-method clause
  :in-layer db-table-layer ((clause check-constraint))
  (with-slots (constraint-name check) clause
    (format nil "CONSTRAINT ~a CHECK ~a" constraint-name check)))




;;; foreign keys

(define-layered-function foreign-keys-statements (class)

  (:method
      :in db-interface-layer ((class db-interface-class))
    (funcall-tables (slot-value class 'tables) #'foreign-keys-statements))

  (:method
      :in db-table-layer ((class db-table-class))
    (with-slots (schema table foreign-keys) class
      (loop for key in foreign-keys
	          collect (statement key)))))


(define-layered-method statement
  :in-layer db-table-layer ((statement composite-key))
  (with-slots (schema keys ref-schema ref-table table columns on-update on-delete) statement
    (let ((key-names (mapcar #'db-syntax-prep keys))
	         (table-name (set-sql-name schema table))
	         (referring-table (set-sql-name ref-schema ref-table))
	         (constraint (format nil "~a_~a_fkey" schema (db-syntax-prep ref-table)))
	         (column-names (mapcar #'db-syntax-prep columns)))
      (format nil "IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '~a') THEN ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY (~{~a~^, ~}) REFERENCES ~a (~{~a~^, ~})~@[ ON UPDATE ~a~]~@[ ON DELETE ~a~]; end if;" constraint referring-table constraint key-names table-name column-names on-update on-delete))))


(define-layered-method statement
  :in-layer db-table-layer ((statement foreign-key))
  (with-slots (schema key ref-schema ref-table table column on-update on-delete) statement
    (let* ((root-key (db-syntax-prep key))
	         (table-name (set-sql-name schema table))
	         (referring-table (set-sql-name ref-schema ref-table))
	         (constraint (format nil "~a_~a_~a_fkey" schema (db-syntax-prep ref-table) root-key))
	         (column-name (db-syntax-prep column)))
      (format nil "IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '~a') THEN ALTER TABLE ~a ADD CONSTRAINT ~a FOREIGN KEY (~a) REFERENCES ~a (~a)~@[ ON UPDATE ~a~]~@[ ON DELETE ~a~]; end if;" constraint referring-table constraint root-key table-name column-name on-update on-delete))))



;; indexing foreign keys

(define-layered-class index
  :in-layer db-table-layer ()
  ((schema :initarg :schema)
   (table :initarg :table)
   (name :initarg :name :initform nil)
   (columns :initarg :columns)))


(define-layered-function index-statement (class)

  (:method
      :in-layer db-interface-layer ((class db-interface-class))
    (princ "indexing...")
    (labels ((to-index (tables acc)
	             (if (null tables)
		               acc
		               (to-index
		                (cdr tables)
		                (let ((statements
			                      (with-active-layers (db-table-layer)
			                        (index-statement (find-class (car tables))))))
		                  (cond ((consp statements)
			                       (nconc statements acc))
			                      ((stringp statements)
			                       (cons statements acc))
			                      (t acc)))))))
      (to-index (slot-value class 'tables) nil)))


  (:method
      :in-layer db-table-layer ((class db-table-class))
    (with-slots (schema table primary-keys) class
      (loop for slot in (filter-slots-by-type class 'db-column-slot-definition)
	          for key = (db-syntax-prep (slot-definition-name slot))
	          unless (member slot primary-keys :test #'eq)
	            when (or (slot-value slot 'foreign-key)
		                   (slot-value slot 'index))
		            collect (clause (make-instance 'index :schema schema
						                                          :table table
						                                          :name (format nil "~a_~a_~a_idx" schema table key)
						                                          :columns key))))))


(define-layered-method clause
  :in-layer db-table-layer ((clause index))
  (with-slots (schema table name columns) clause
    (format nil "CREATE INDEX IF NOT EXISTS~@[ ~a~] ON ~a.~a (~{~a~^, ~});" name schema table (ensure-list columns))))


;;; create type


(define-layered-function create-pg-composite (class)
  (:documentation "Creating an explicit type associated with a class,
allows the values to be expressed as an array within the arglist of a
procedure. It is a simple matter then of calling unnest, using a positional
parameter to reference the array. This is particularly useful when adding
multiple records in a one-to-many relationship.")

  (:method
      :in db-interface-layer ((class db-interface-class))
    (with-slots (tables) class
      (loop
	      for table in tables
	      for required = (require-columns (find-class table))
	      for composite = (when required
			                    (with-active-layers (db-table-layer)
			                      (create-pg-composite (find-class table))))
	      when composite
	        collect composite)))

  (:method
      :in db-table-layer ((class db-table-class))
    (with-slots (schema table require-columns) class
      (princ (format nil "Creating composite type: ~a_type~%" (as-prefix table)))
      (let ((table-name (set-sql-name schema (as-prefix table))))
	      (format nil "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '~a_type') THEN CREATE TYPE ~a_type AS (~{~{~a ~a~}~^, ~}); END IF;"
		            (as-prefix table) table-name (mapcar #'(lambda (column)
							                                           (list (column-name column)
								                                               (get-column-type column)))
							                                       require-columns))))))


;;; create domain

(define-layered-function create-typed-domain (class)
  (:documentation "Creates explicit domains associated with respective columns.
As domains are typed they are useful to generate dynamic overloaded sql procedures
so that differing columns of the same type can be applied to a procedure call.")

  (:method
      :in db-interface-layer ((class db-interface-class))
    (with-slots (tables) class
      (loop
	      for table in tables
	      for domain = (with-active-layers (db-table-layer)
		                   (create-typed-domain (find-class table)))
	      when domain
	      collect domain)))

  (:method
      :in db-table-layer ((class db-table-class))
    (with-slots (schema table) class
      (format nil "~{~a~}"
	            (let ((columns (filter-slots-by-type class 'db-column-slot-definition)))
		            (loop
		              for column in columns
		              do (princ (format nil "Creating domain: ~s~%" (slot-value column 'domain)))
		              collect (with-slots (domain col-type) column
			                      (format nil "IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '~a') THEN CREATE DOMAIN ~a.~a AS ~a; END IF;"
				                            domain schema domain (get-column-type column)))))))))


(defmethod get-column-type ((column enumerated-column-slot-definition))
  (with-slots (store-index col-type) column
    (if store-index col-type (format nil "enum_~a" (column-name column)))))

(defmethod get-column-type ((column date/time-column-slot-definition))
  (with-slots (col-type) column
    (case col-type
      ((:time-with-time-zone :time-without-time-zone)
       (string-upcase (substitute #\space #\- (symbol-name col-type))))
      (t col-type))))

(defmethod get-column-type ((column db-column-slot-definition))
  (with-slots (col-type) column
    (if (eq col-type :serial) :integer col-type)))

;;; setting up

(define-layered-function build-db-component (class name &rest function-list)
  (:documentation "Builds database components required by class")

  (:method
      :in db-layer (class (name string) &rest function-list)
    (let ((procedure (make-instance 'procedure
				                            :schema (slot-value class 'schema)
				                            :name name)))
      (setf (slot-value procedure 'sql-list)
	          (reduce #'(lambda (a b)
			                  (nconc a b))
		                (mapcan #'(lambda (fn)
				                        (list (funcall fn class)))
			                      function-list)))
      (set-control procedure)
      (values
       (sql-statement (statement procedure))
       procedure))))

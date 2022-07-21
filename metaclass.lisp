(in-package stw.db)


(define-layered-class db-class
  :in db-layer (base-class)
  ())

;;; DB-INTERFACE-LAYER metaclasses

(define-layered-class db-wrap
  :in db-interface-layer (db-class)
  ((schema
    :initarg :schema
    :initform "public"
    :reader schema)
   (tables
    :initarg :tables
    :initform nil
    :accessor tables)
   (maps
    :initform nil
    :type list
    :reader maps))
  (:documentation "Aggregate tables for transactions, cte's, etc. Map to application objects."))


(defmethod partial-class-base-initargs append ((class db-wrap))
  '(:tables))

(defclass db-base-column-definition
    (stw-direct-slot-definition)
  ())

(defclass db-aggregate-slot-definition (db-base-column-definition)
  ((maps
    :initarg :maps-table
    :initarg :maps-column
    :initarg :maps-columns
    :reader maps)
   (express-as-type
    :initarg :express-as-type
    :initform nil
    :type (or null symbol)
    :documentation "Set requested type for SELECT operations to return. Unless otherwise specified, the type
set in maps-table will be returned.")
   (constraint
    :initarg :constraint
    :initform nil
    :type (or null cons)
    :documentation "similar to SET-MAPPED-DEFAULTS constraint references other columns in a
mapped table. However, CONSTRAINT requires a form, other than a list, e.g. (string= \"value\") or 
(> 3). Multiple constraints can be set in the form so that a constraint could reference a number greater than
and less than. The list will be walked, using INFIX-LIST, setting the appropriate operand and column name")
   (set-mapped-defaults
    :initarg :set-mapped-defaults
    :initform nil :type (or null cons)
    :documentation "when mapping a column, other columns from the same table may have a fixed value. 
Set as alist ((COLUMN . VALUE))")))

(defmethod slot-definition-class ((class stw-interface))
  'db-aggregate-slot-definition)


;;; DB-TABLE-LAYER metaclasses

(define-layered-class db
  :in db-table-layer (singleton-class db-class)
  ((schema :initarg :schema :initform "public" :reader schema :type string)
   (table :initarg :table :initform nil :reader table :type string)
   (primary-keys :initarg :primary-keys :accessor primary-keys :type (or null cons))
   (foreign-keys :initarg :foreign-keys :initform nil :accessor foreign-keys :type (or null cons))
   (constraints :initarg :constraints :initform nil :reader constraints :type (or null cons))
   (mapped-by :initform nil :reader mapped-by :type (or null cons))
   (require-columns :type (or null string) :reader require-columns)))


(defmethod partial-class-base-initargs append ((class db))
  '(:schema :table :primary-keys :foreign-keys :constraints))


(defclass db-column-slot-definition (db-base-column-definition)
  ((schema :initform "public" :type string :reader schema)
   (table-class :initform nil :reader table-class)
   (table :initarg :table :initform nil :reader table)
   (col-type :initarg :col-type :initform :text :reader col-type :type keyword)
   (domain :reader domain :type string)
   (primary-key :initarg :primary-key :initform nil :type boolean)
   (foreign-key :initarg :foreign-key :initform nil :reader foreign-key :type (or cons null))
   (unique :initarg :unique :initform nil :type (or boolean null))
   (check :initarg :check :initform nil :type (or null cons))
   (default :initarg :default :reader default)
   (index :initarg :index :initform nil :reader index :type boolean)
   (not-null :initarg :not-null :initform nil :type boolean :reader not-null-p)
   (value :initarg :value :initform nil)
   (mapped-by :initform nil :reader mapped-by)
   (column-name :reader column-name)
   (lock-value :initarg :lock-value :initform nil :reader lock-value)))



(defmethod slot-definition-class ((class stw-table))
  'db-column-slot-definition)

(define-layered-class db-interface-class
  :in-layer db-interface-layer (stw-base-class db-wrap) ())

(define-layered-class db-table-class
  :in-layer db-table-layer (stw-base-class db) ())

(define-layered-class db-key-table
  :in-layer db-table-layer (db-table-class)
  ()
  (:documentation "Specialised type for tables 
with a single column of type serial."))




;;;;;;; Initialization Methods

(defstruct (slot-mapping (:conc-name nil))
  (mapping-node nil :type (or null db-interface-class))
  (mapping-slot nil :type db-aggregate-slot-definition)
  (mapped-table nil :type db-table-class)
  (mapped-column nil :type db-column-slot-definition)
  (mapped-columns () :type list))


(define-layered-method initialize-in-context
  :in db-interface-layer ((slot db-aggregate-slot-definition) 
			  &key maps-table maps-column maps-columns)
  (with-slots (maps express-as-type) slot
    (when maps-table
      (unless (find-class maps-table)
	(error "the table ~a specified in maps-table does not exist" maps-table))
      (unless (or maps-column maps-columns)
	(warn "No value set for MAPS-COLUMNS or MAPS-COLUMN for slot ~a." (slot-definition-name slot)))
      (setf maps (make-slot-mapping 
		  :mapping-slot slot
		  :mapped-table (find-class maps-table)
		  :mapped-column (find-slot-definition maps-table maps-column 'db-column-slot-definition)
		  :mapped-columns (loop for column in maps-columns
					collect (find-slot-definition maps-table column 'db-column-slot-definition))))
      (unless express-as-type
	(setf (slot-value slot 'express-as-type) maps-table)))))


(define-layered-class foreign-key
  :in-layer db-table-layer ()
  ((table :initarg :table :reader table)
   (column :initarg :column :reader column)
   (key :initarg :key :initform nil :reader key)
   (ref-schema :initarg :ref-schema :initform nil :reader ref-schema)
   (ref-table :initarg :ref-table :initform nil :reader ref-table)
   (schema :initarg :schema :initform nil :reader schema)
   (on-update :initarg :on-update :initform nil :reader on-update)
   (on-delete :initarg :on-delete :initform nil :reader on-delete)
   (no-join :initarg :no-join :initform nil :type boolean :reader no-join)))


(defmethod shared-initialize :after ((class foreign-key) slot-names &rest initargs &key table column schema ref-schema on-update on-delete)
  (unless (and table column)
    (error "Foreign key plist must contain both :TABLE and :COLUMN params"))
  (unless schema
    (setf (slot-value class 'schema) (schema (find-class table))))
  (unless ref-schema
    (setf (slot-value class 'ref-schema) (slot-value class 'schema)))
  (flet ((on-action (action)
	   (when action
	     (unless (member action '(:restrict :cascade :no-action :set-null :set-default))
	       (error "~a is not a keyword. Accepted values include :RESTRICT :CASCADE :NO-ACTION :SET-NULL :SET-DEFAULT"
		      action)))))
    (on-action on-update)
    (on-action on-delete)))


(define-layered-method initialize-in-context
  :in db-table-layer ((slot db-column-slot-definition)
		      &key default col-type check primary-key foreign-key &allow-other-keys)
  (let ((slot-name (slot-definition-name slot)))
    (when (eq col-type 'serial)
      (setf (slot-value slot 'lock-value) t))
    (flet ((set-not-null ()
	     (unless (eq col-type 'serial)
	       (setf (slot-value slot 'not-null) t))))
      (when primary-key
	(set-not-null)))
    (when check
      (setf (slot-value slot 'check)
	    (infill-column check slot-name)))
    (when foreign-key
      (let ((schema (getf foreign-key :schema)))
	(let ((f-key (apply #'make-instance 'foreign-key
			    :schema schema
			    :ref-schema (or (getf foreign-key :ref-schema)
					    schema)
			    :key slot-name
			    foreign-key)))
	  (setf (slot-value slot 'foreign-key) f-key))))))



(defun sort-tables (backtrace-alist)
  (let ((acc))
    (map-tree-depth-first
     #'(lambda (item)
	 (cond ((member item acc)
		nil)
	       (t (push item acc)
		  item)))
     (nreverse
      (sort backtrace-alist
	    #'(lambda (a b)
		(member (car a) (cdr b) :test #'eq))))
     t)))


(defun ensure-bound-tables (tables sorted-tables)
  "Ensure all tables are bound by means of a foreign-key reference."
  (loop
    for table in tables
    unless (member table sorted-tables :test #'eq)
      collect table))


(define-layered-method initialize-in-context
  :in db-interface-layer ((class db-wrap) &key)
  (with-slots (foreign-keys tables) class

    ;; Read relevant precedents into tables and each tables foreign-keys
    ;; into the nodes foreign-key slot. Backtrace-table and f-key-table
    ;; are used for sorting foreign keys based on mutual dependencies.
    (let* ((backtrace-table)
	   (precedents (filter-precedents-by-type class 'db-table-class))
	   (named-precedents (mapcar #'class-name precedents)))
      (flet ((collate-keys (table-class)
	       (loop
		 for fkey in (slot-value table-class 'foreign-keys)
		 do (with-slots (ref-table table) fkey

		      ;; A table referenced by a foreign key is not necessarily
		      ;; a precedent of an interface node. Filter accordingly.
		      (when (member table named-precedents :test #'eq)
			(aif (assoc table backtrace-table :test #'eq)
			     (pushnew ref-table (cdr self))
			     (setf backtrace-table (acons table (list (class-name table-class)) backtrace-table))))))))
	(loop
	  for object in precedents

	  ;; set schema and tables and collate foreign-keys
	  when (and (string= (slot-value class 'schema) "public")
		    (slot-value object 'schema))
	    do (setf (slot-value class 'schema) (slot-value object 'schema))
	  do (pushnew (class-name object) tables :test #'eq)
	  do (collate-keys object))

	;; add tables mapped by aggregator slots and push
	;; mappings to class, table and slot definitions.
	(loop
	  for slot in (filter-slots-by-type class 'db-aggregate-slot-definition)
	  do (with-slots (maps) slot
	       (with-slots (mapping-node mapped-table mapped-column) maps
		 (setf mapping-node class)
		 (pushnew (class-name mapped-table) tables :test #'eq)
		 (collate-keys mapped-table)
		 (pushnew maps (slot-value class 'maps) :test #'eq)
		 (pushnew maps (slot-value mapped-column 'mapped-by) :test #'eq)
		 (pushnew maps (slot-value mapped-table 'mapped-by) :test #'eq))))
	(let ((sorted-tables (sort-tables backtrace-table)))
	  (awhen (ensure-bound-tables tables sorted-tables)
	    (warn "the table(s) ~{~a^ ~} are not bound. They either 
don't belong in this node or a foreign key is required" self))
	  (setf tables sorted-tables))))))



(define-layered-method initialize-in-context
  :in db-table-layer ((class db) &key)
  (with-slots (schema foreign-keys constraints table) class
    (mapcar #'(lambda (slot)
		(slot-makunbound class slot))
	    '(primary-keys require-columns))
    (unless table
      (setf table (funcall *reserved-keywords-filter* (db-syntax-prep (class-name class)))))
    (map-filtered-slots
     class
     #'(lambda (slot) (typep slot 'db-column-slot-definition))
     #'(lambda (slot) 
	 (let ((slot-name (slot-definition-name slot))
	       (to-check))

	   (with-slots (domain table-class column-name foreign-key col-type check) slot
	     (setf column-name (funcall *reserved-keywords-filter* (db-syntax-prep slot-name))
		   (slot-value slot 'table) table
		   table-class class
		   domain (funcall *reserved-function/type-filter*
				   (format nil "~a_~a"
					   (db-syntax-prep (class-name class))
					   (db-syntax-prep slot-name)))
		   (slot-value slot 'schema) schema)
	     (when foreign-key
	       (with-slots (ref-table table) foreign-key
		 (setf ref-table (class-name class))
		 (unless (member slot-name (mapcar #'key foreign-keys) :test #'eq)
		   (pushnew foreign-key foreign-keys :test #'eq))))

	     ;; check constraints
	     (when check
	       (setf (getf to-check :check) check
		     (getf to-check :col-name) slot-name
		     (getf to-check :table) table)
	       (pushnew to-check constraints :test #'equal))))))))



(defmethod slot-unbound (class (instance db) (slot-name (eql 'primary-keys)))
  (loop
    for slot in (filter-slots-by-type instance 'db-column-slot-definition)
    when (slot-value slot 'primary-key)
      collect slot into keys
    finally (return (setf (slot-value instance 'primary-keys) keys))))


(defmethod slot-unbound (class (instance db) (slot-name (eql 'require-columns)))
  (loop
    for slot in (filter-slots-by-type instance 'db-column-slot-definition)
    for foreign-key = (slot-value slot 'foreign-key)
    when (and foreign-key (slot-value foreign-key 'no-join))
      collect slot into require-columns%
    unless (or foreign-key
	       (let ((col-type (slot-value slot 'col-type)))
		 (or (eq col-type :serial)
		     (eq col-type :timestamptz))))
    collect slot into require-columns%
    finally (return (setf (slot-value instance 'require-columns) require-columns%))))
		  

(define-layered-function get-root-key (class)
  (:method
      :in db-layer ((class db-wrap))
    (let ((root-table (find-class (car (tables class)))))
	(slot-value root-table 'primary-keys))))
  

(define-layered-function find-column-slot (class slot-name)
  (:method
      :in db-layer ((class db-interface-class) slot-name)
    (awhen (find-slot-definition class slot-name 'db-base-column-definition)
      (typecase self
	(db-column-slot-definition
	 self)
	(db-aggregate-slot-definition
	 (mapped-column (slot-value self 'maps)))))))
   

(defmacro define-db-class (name layer metaclass &body body)
  (unless (serialized-p (car body))
    (push 'serialize (car body)))
  `(eval-when (:compile-toplevel :load-toplevel :execute)
     (define-base-class ,name
       :in ,layer
       ,@body
       (:metaclass ,metaclass))))


(defmacro define-key-table (name &body body)
  "A key table is a single column table with 
autoincrementing values, defined as a separate
type purely for convenience and to enable 
dispatching on type."
  (let ((column (ensure-list (cadr body))))
    (setf (getf (cdr column) :col-type) :serial
	  (getf (cdr column) :primary-key) t
	  (cadr body) (list column))
    `(define-db-class ,name db-table-layer db-key-table
       ,@body)))


(defmacro define-db-table (name &body body)
  `(define-db-class ,name db-table-layer db-table-class
     ,@body))

(defmacro define-interface-node (name &body body)
  (let ((metaclass
	  (aif (cddr body)
	       (aif (assoc :metaclass self)
		    (prog1
			(cadr self)
		      (setf (cddr body) (delete self (cddr body))))
		    'db-interface-class)
	       'db-interface-class)))
    `(define-db-class ,name db-interface-layer ,metaclass
       ,@body)))


(defmethod slot-unbound ((class db-interface-class) instance slot-name)
  nil)

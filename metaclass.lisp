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
   (root-key
    :initform nil
    :initarg :root-key
    :documentation "As a primary key is to a table, a root key is to a node. All referenced tables must have a foreign key referencing the root key column. Amongst other purposes, it represents a slot of equal value between two instances of the same class and serves as an anchor during updating operations. Once bound, it's value should not be updated. E.g. an ID column."
    :reader root-key)
   (maps
    :initform nil
    :type list
    :reader maps))
  (:documentation "Aggregate tables for transactions, cte's, etc. Map to application objects."))


(defmethod partial-class-base-initargs append ((class db-wrap))
  '(:tables :root-key))

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
    :type (null symbol)
    :documentation "Set requested type for SELECT operations to return. Unless otherwise specified, the type
set in maps-table will be returned.")
   (constraint
    :initarg :constraint
    :initform nil
    :type (null cons)
    :documentation "similar to SET-MAPPED-DEFAULTS constraint references other columns in a
mapped table. However, CONSTRAINT requires a form, other than a list, e.g. (string= \"value\") or 
(> 3). Multiple constraints can be set in the form so that a constraint could reference a number greater than
and less than. The list will be walked, using INFIX-LIST, setting the appropriate operand and column name")
   (set-mapped-defaults
    :initarg :set-mapped-defaults
    :initform nil :type (null cons)
    :documentation "when mapping a column, other columns from the same table may have a fixed value. 
Set as alist ((COLUMN . VALUE))")))

(defmethod slot-definition-class ((class stw-interface))
  'db-aggregate-slot-definition)


;;; DB-TABLE-LAYER metaclasses

(define-layered-class db
  :in db-table-layer (singleton-class db-class)
  ((schema :initarg :schema :initform "public" :reader schema :type string)
   (table :initarg :table :initform nil :reader table :type string)
   (primary-keys :initarg :primary-keys :initform nil :accessor primary-keys :type (null cons))
   (foreign-keys :initarg :foreign-keys :initform nil :accessor foreign-keys :type (null cons))
   (referenced-by :initarg :referenced-by :initform nil :accessor referenced-by :type (cons null))
   (constraints :initarg :constraints :initform nil :reader constraints :type (null cons))
   (mapped-by :initform nil :reader mapped-by :type (null cons))
   (require-columns :type (null string) :reader require-columns)))


(defmethod partial-class-base-initargs append ((class db))
  '(:schema :table :primary-keys :foreign-keys :referenced-by :constraints))


(defclass db-column-slot-definition (db-base-column-definition)
  ((schema :initform "public" :type string :reader schema)
   (table-class :initform nil :reader table-class)
   (table :initarg :table :initform nil :reader table)
   (col-type :initarg :col-type :initform :text :reader col-type :type keyword)
   (domain :reader domain :type string)
   (primary-key :initarg :primary-key :initform nil :type boolean)
   (root-key :initarg :root-key :initform nil :type boolean)
   (foreign-key :initarg :foreign-key :initform nil :reader foreign-key :type (cons null))
   (unique :initarg :unique :initform nil :type (boolean null))
   (check :initarg :check :initform nil :type (null cons))
   (default :initarg :default :initform nil)
   (index :initarg :index :initform nil :reader index :type boolean)
   (not-null :initarg :not-null :initform nil :type boolean :reader not-null-p)
   (referenced :initarg :referenced :initform nil :type boolean)
   (value :initarg :value :initform nil)
   (mapped-by :initform nil :reader mapped-by)
   (column-name :reader column-name)))



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



(define-layered-class root-key
  :in-layer db-interface-layer ()
  ((table :initarg :table :reader table)
   (column :initarg :column :reader column)))


(define-layered-class foreign-key
  :in-layer db-table-layer (root-key)
  ((key :initarg :key :initform nil :reader key)
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
		      &key col-type check primary-key foreign-key root-key &allow-other-keys)
  (let ((slot-name (slot-definition-name slot)))

    (flet ((process-primary-key ()
	     (unless (eq col-type 'serial)
	       (setf (slot-value slot 'not-null) t))))

      (when root-key
	(setf (slot-value slot 'primary-key) t)
	(process-primary-key))

      (when primary-key
	(process-primary-key)))

    (when check
      (setf (slot-value slot 'check)
	    (infill-column check slot-name)))

    (when foreign-key
      (let ((schema (getf foreign-key :schema)))
	(setf (slot-value slot 'foreign-key)
	      (apply #'make-instance 'foreign-key
		     :schema schema
		     :ref-schema (or (getf foreign-key :ref-schema)
				     schema)
		     :key slot-name
		     foreign-key))))))



(defun sort-tables (backtrace-alist)
  (let ((acc))
    (stw.util:map-tree-depth-first
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


(define-layered-function ensure-bound-columns (class)
  (:documentation "Ensure all tables are bound by means of a key column")

  (:method
      :in db-interface-layer ((class db-wrap))
    (with-slots (tables root-key) class
      (let* ((acc)
	     (referring-table (slot-value root-key 'table))
	     (referenced-by (slot-value referring-table 'referenced-by)))
	(when referenced-by
	  (loop
	    for fkey in referenced-by
	    do (pushnew (slot-value fkey 'ref-table) acc)))
	(reduce #'set-difference (list tables acc (list (class-name referring-table))))))))


(define-layered-method initialize-in-context
  :in db-interface-layer ((class db-wrap) &key)
  (with-slots (root-key foreign-keys tables) class

    ;; Read relevant precedents into tables and each tables foreign-keys
    ;; into the nodes foreign-key slot. Backtrace-table and f-key-table
    ;; are used for sorting foreign keys based on mutual dependencies.
    (let (backtrace-table)
      (flet ((collate-keys (table-class)
	       (loop
		 for key in (slot-value table-class 'foreign-keys)
		 do (with-slots (table column) key
		      (let ((ref-slot (find-slot-definition table column 'db-column-slot-definition)))

			;; Find root-key amongst foreign-keys and
			;; set root-key for class
			(when (slot-value ref-slot 'root-key)
			  (setf root-key (make-instance 'root-key :table (find-class table)
								  :column ref-slot))))
		      (aif (assoc table backtrace-table :test #'eq)
			   (pushnew (class-name table-class) (cdr self))
			   (setf backtrace-table (acons table (list (class-name table-class)) backtrace-table)))))))
	(loop
	  for object in (filter-precedents-by-type class 'stw-base-class)

	  ;; set schema and tables and collate foreign-keys
	  when (and (string= (slot-value class 'schema) "public")
		    (slot-value object 'schema))
	    do (setf (slot-value class 'schema) (slot-value object 'schema))
	  when (typep object 'db-table-class)
	    do (pushnew (class-name object) tables :test #'eq)
	    and do (collate-keys object))

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
	(setf tables (sort-tables backtrace-table)))))
  ;; and ensure each column is tied to the root-key
  (awhen (ensure-bound-columns class)
    (error "the table(s) ~{~a^ ~} are not bound to a root-key. 
They either don't belong in this node or a foreign key is required" self)))



(define-layered-method initialize-in-context
  :in db-table-layer ((class db) &key)
  (with-slots (schema foreign-keys constraints table) class
    (mapcar #'(lambda (slot)
		(slot-makunbound class slot))
	    '(primary-keys require-columns))
    (unless table
      (setf table (db-syntax-prep (class-name class))))
    (map-filtered-slots
     class
     #'(lambda (slot) (typep slot 'db-column-slot-definition))
     #'(lambda (slot) 
	 (let ((slot-name (slot-definition-name slot))
	       (to-check))

	   (with-slots (domain table-class column-name foreign-key col-type referenced check) slot
	     (setf column-name (db-syntax-prep slot-name)
		   (slot-value slot 'table) table
		   table-class class
		   domain (format nil "~a_~a" table column-name)
		   (slot-value slot 'schema) schema)
	     (when foreign-key
	       (with-slots (ref-table table) foreign-key
		 (setf ref-table (class-name class))
		 (unless (member slot-name (mapcar #'key foreign-keys) :test #'eq)
		   (pushnew foreign-key foreign-keys :test #'eq)
		   (pushnew foreign-key (slot-value (find-class table) 'referenced-by)
			    :test #'eq))))

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
      collect slot))


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
	  (getf (cdr column) :root-key) t
	  (getf (cdr column) :referenced) t
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

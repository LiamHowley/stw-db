(in-package stw.db)


;;; DB-INTERFACE-LAYER metaclasses

(define-layered-class db-wrap
  :in db-interface-layer (base-class)
  ((schema
    :initarg :schema
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
    :initarg :maps-columns)
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
  :in db-table-layer (singleton-class base-class)
  ((schema :initarg :schema :initform "public" :reader schema :type string)
   (table :initarg :table :initform nil :reader table :type string)
   (primary-keys :initarg :primary-keys :initform nil :accessor primary-keys :type (null cons))
   (foreign-keys :initarg :foreign-keys :initform nil :accessor foreign-keys :type (null cons))
   (referenced-by :initarg :referenced-by :initform nil :accessor referenced-by :type (cons null))
   (referenced-columns :initarg :referenced-columns :initform nil :accessor referenced-columns :type (cons null))
   (constraints :initarg :constraints :initform nil :reader constraints :type (null cons))
   (mapped-by :initform nil :reader mapped-by :type (null cons))
   (require-columns :initform nil :type (null string) :reader require-columns)))


(defmethod partial-class-base-initargs append ((class db))
  '(:schema :table :primary-keys :foreign-keys :referenced-by :constraints))


(defclass db-column-slot-definition (db-base-column-definition)
  ((schema :initform nil :type string)
   (table-class :initform nil :reader table-class)
   (table :initarg :table :initform nil :reader table)
   (col-type :initarg :col-type :initform :text :reader col-type :type keyword)
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
  (mapping-node nil :type db-interface-class)
  (mapping-slot nil :type db-aggregate-slot-definition)
  (mapped-table nil :type db-table-class)
  (mapped-column nil :type db-column-slot-definition)
  (mapped-columns () :type list))


(defmethod map-column-p ((column db-column-slot-definition) maps-column maps-columns)
  (unless (member slot (slot-value column 'mapped-by) :test #'eq)
    (let ((name (slot-definition-name column)))
      (or (eq name maps-column)
	  (member name maps-columns)))))


(defmethod default-column-map ((slot db-aggregate-slot-definition) maps-table maps-columns)
  (with-slots (maps) slot
    (setf maps
	  (list :maps-table maps-table
		:maps-columns (mapcan #'(lambda (column)
					  (list column))
				      (filter-slots-by-type (find-class maps-table) 'db-column-slot-definition))))))



(define-layered-method initialize-in-context
  :in db-interface-layer ((slot db-aggregate-slot-definition) slot-names
							      &key maps-table maps-column maps-columns)
  (declare (ignore slot-names))
  (with-slots (maps express-as-type) slot
    (let ((slot-name (slot-definition-name slot)))

      (when maps-table
	;; maps table must correspond to a class
	(unless (find-class maps-table)
	  (error "the table ~a specified in maps-table of slot ~a does not exist" maps-table (slot-definition-name slot)))

	;; ensure mapping
	(unless (or maps-column maps-columns)
	  (warn "No value set for MAPS-COLUMNS or MAPS-COLUMN for slot ~a. All columns without foreign-keys of table ~a will be mapped." slot-name maps-table)
	  (default-column-map slot maps-table maps-columns))

	(setf maps (list maps-table maps-columns maps-column)))

      (unless express-as-type
	(setf (slot-value slot 'express-as-type) maps-table)))))



;;(defmethod shared-initialize :after ((slot db-column-slot-definition) slot-names
;;				     &key col-type table check primary-key referenced foreign-key &allow-other-keys)
;;  (declare (ignore slot-names))

(define-layered-method initialize-in-context
  :in db-table-layer ((slot db-column-slot-definition)
		      slot-names &key col-type table check primary-key referenced foreign-key root-key &allow-other-keys)
  (declare (ignore slot-names))
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

    (flet ((test-foreign-key (key)
	     (unless (and (getf key :table)
			  (getf key :column))
	       (error "Foreign key plist must contain both :TABLE and :COLUMN params"))
	     (unless (getf key :schema)
	       (setf (getf foreign-key :schema) (schema (find-class (getf key :table)))))
	     (flet ((on-action (action)
		      (when action
			(unless (member action '(:restrict :cascade :no-action :set-null :set-default))
			  (error "~a is not a keyword. Accepted values include :RESTRICT :CASCADE :NO-ACTION :SET-NULL :SET-DEFAULT"
				 action)))))
	       (let ((on-update (getf key :update))
		     (on-delete (getf key :delete)))
		 (on-action on-update)
		 (on-action on-delete)))))

      (when foreign-key
	(test-foreign-key foreign-key)))))



(defun sort-tables (tables backtrace-table)
  (loop
    until (eql (hash-table-count backtrace-table) 0)
    do (maphash #'(lambda (key values)
		    (let ((match))
		      (typecase values
			(cons
			 (loop
			   for value in values
			   when (gethash value backtrace-table)
			     do (setf (gethash key backtrace-table) value)
			     and do (setf match t)))
			(symbol
			 (when (gethash values backtrace-table)
			   (setf match t))))
		      (unless match
			(setf tables (remove key tables :test #'eq))
			(push key tables)
			(remhash key backtrace-table))))
		backtrace-table))
  tables)


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
   (no-join :initarg :no-join :initform nil :reader no-join)))


(define-layered-method initialize-in-context
  :in db-interface-layer ((class db-wrap) slot-names &key &allow-other-keys)
  (declare (ignore slot-names))
  (with-slots (root-key foreign-keys tables) class

    ;; Read relevant precedents into tables and each tables foreign-keys
    ;; into the nodes foreign-key slot. Backtrace-table and f-key-table
    ;; are used for sorting foreign keys based on mutual dependencies.
    (let ((backtrace-table (make-hash-table :test #'eq)))
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
		      (if (gethash table backtrace-table)
			  (pushnew (class-name table-class) (gethash table backtrace-table))
			  (setf (gethash table backtrace-table) (list (class-name table-class))))))))
	(loop
	  for object in (filter-precedents-by-type class 'stw-base-class)

	  ;; set schema and tables and collate foreign-keys
	  unless (slot-boundp class 'schema)
	    do (setf (slot-value class 'schema) (slot-value object 'schema))
	  when (typep object 'db-table-class)
	    do (pushnew (class-name object) tables :test #'eq)
	    and do (collate-keys object))

	;; add tables mapped by aggregator slots and update
	;; mappings to reflect class and slot definitions.
	(loop
	  for slot in (filter-slots-by-type class 'db-aggregate-slot-definition)
	  do (with-slots (maps) slot
	       (when (consp maps)
		 (destructuring-bind (maps-table maps-columns maps-column) maps
		   (pushnew maps-table tables :test #'eq)
		   (setf maps-table (find-class maps-table)
			 maps-column (when maps-column
				       (find-slot-definition maps-table maps-column 'db-column-slot-definition))
			 maps-columns (loop for column in maps-columns
					    collect (find-slot-definition maps-table column 'db-column-slot-definition)))
		   (collate-keys maps-table)

		   ;; let the respective table and column know it is being mapped and by whom
		   (let ((column-map (make-slot-mapping :mapping-node class
							:mapping-slot slot
							:mapped-table maps-table
							:mapped-column maps-column
							:mapped-columns maps-columns)))
		     (pushnew column-map (slot-value class 'maps) :test #'eq)
		     (pushnew column-map (slot-value maps-column 'mapped-by) :test #'eq)
		     (pushnew column-map (slot-value maps-table 'mapped-by) :test #'eq)
		     (setf maps column-map)))))))

      ;; now sort the tables
      (setf tables (sort-tables tables backtrace-table))))
  ;; and ensure each column is tied to the root-key
  (awhen (ensure-bound-columns class)
    (error "the table(s) ~{~a^ ~} are not bound to a root-key. 
They either don't belong in this node or a foreign key is required" self)))


(define-layered-method initialize-in-context
  :in db-table-layer ((class db) slot-names &key &allow-other-keys)
  (declare (ignore slot-names))
  (with-slots (schema primary-keys foreign-keys constraints table referenced-columns require-columns) class
    (unless table
      (setf table (db-syntax-prep (class-name class))))
    (loop for slot in (filter-slots-by-type class 'db-column-slot-definition)
	  for slot-name = (slot-definition-name slot)
	  for to-check = nil

	  ;; primary key 
	  do (with-slots (table-class primary-key column-name foreign-key col-type referenced check) slot
	       (setf column-name (db-syntax-prep slot-name)
		     (slot-value slot 'table) table
		     table-class class)
	       (when primary-key
		 (pushnew slot primary-keys :test #'eq))
	       (when referenced
		 (pushnew (list column-name col-type) referenced-columns :test #'equal))

	       ;;foreign-key and require-columns
	       (cond (foreign-key
		      (destructuring-bind (&key table column on-delete on-update no-join) foreign-key
			(let ((key-class (apply #'make-instance 'foreign-key
						:schema schema
						:ref-schema schema
						:ref-table (class-name class)
						:key slot-name
						foreign-key)))
			  (when no-join
			    (pushnew slot require-columns :test #'eq))
			  (unless (member slot-name (mapcar #'key foreign-keys) :test #'eq)
			    (pushnew key-class foreign-keys :test #'eq)
			    (pushnew key-class (slot-value (find-class table) 'referenced-by)
				     :test #'eq)))))
		     (t
		      (unless (or (eq col-type :serial)
				  (eq col-type :timestamptz))
			(pushnew slot require-columns :test #'eq))))
	       ;; check constraints
	       (when check
		 (setf (getf to-check :check) check
		       (getf to-check :col-name) slot-name
		       (getf to-check :table) table)
		 (pushnew to-check constraints :test #'equal))))

    ;; finish
    (awhen primary-keys
      (setf primary-keys (nreverse self)))))


(defun serialized-p (supers)
  (and supers
       (loop for class in supers
	       thereis (filter-precedents-by-type class 'singleton-class))))

(defmacro define-db-class (name layer metaclass &body body)
  (unless (serialized-p (car body))
    (push 'serialize (car body)))
  `(eval-when (:compile-toplevel :load-toplevel :execute)
     (define-base-class ,name
       :in ,layer
       ,@body
       (:metaclass ,metaclass))))


(defmacro define-key-table (name &body body)
  `(define-db-class ,name db-table-layer db-key-table
     ,@body))

(defmacro define-db-table (name &body body)
  `(define-db-class ,name db-table-layer db-table-class
     ,@body))

(defmacro define-interface-node (name &body body)
  `(define-db-class ,name db-interface-layer db-interface-class
     ,@body))

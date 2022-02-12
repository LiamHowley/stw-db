(in-package stw.db)


;;; DB-INTERFACE-LAYER metaclasses

(define-layered-class db-wrap
  :in db-interface-layer (base-class)
  ((tables
    :initarg :tables
    :initform nil
    :accessor tables)
   (key-columns
    :initarg :key-columns
    :initform nil
    :documentation "Slots of equal value between two instances of the same class. Anchor for updating queries. Typically referenced by multiple columns, and is not updated. E.g. an ID column."
    :reader key-columns)
   (foreign-keys
    :initarg :foreign-keys
    :initform nil
    :documentation "All foreign keys in interface node. Require referenced table and referenced column for each key."
    :accessor foreign-keys
    :type (null cons))
   (maps
    :initform nil
    :type list))
  (:documentation "Aggregate tables for transactions, cte's, etc. Map to application objects."))


(defmethod partial-class-base-initargs append ((class db-wrap))
  '(:tables :key-columns))

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



;;; DB-TABLE-LAYER metaclasses

(define-layered-class db
  :in db-table-layer (base-class)
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
   (table :initarg :table :initform nil :type symbol)
   (col-type :initarg :col-type :initform :text :reader col-type :type keyword)
   (primary-key :initarg :primary-key :initform nil :type boolean)
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



(defmethod initialize-instance :after ((slot db-aggregate-slot-definition) &key maps-table maps-column maps-columns)
  (with-slots (maps express-as-type) slot
    (let ((slot-name (slot-definition-name slot)))
      ;; maps table must correspond to a class
      (unless (and maps-table (find-class maps-table))
	(error "the table ~a specified in maps-table of slot ~a does not exist" maps-table (slot-definition-name slot)))

      ;; ensure mapping
      (unless (or maps-column maps-columns)
	(warn "No value set for MAPS-COLUMNS or MAPS-COLUMN for slot ~a. All columns without foreign-keys of table ~a will be mapped."
	      slot-name maps-table)
	(default-column-map slot maps-table maps-columns))

	(setf maps (list maps-table maps-columns maps-column))

      (unless express-as-type
	(setf (slot-value slot 'express-as-type) maps-table)))))



(defmethod shared-initialize :after ((slot db-column-slot-definition) slot-names
				     &key col-type table check primary-key referenced foreign-key &allow-other-keys)
  (declare (ignore slot-names))
  (let ((slot-name (slot-definition-name slot)))

    (when primary-key
      (unless (eq col-type 'serial)
	(setf (slot-value slot 'not-null) t)))

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


(defmethod shared-initialize :around ((class db-wrap) slot-names
				      &key &allow-other-keys)
  (declare (ignore slot-names))
  (call-next-method)
  (with-slots (key-columns foreign-keys tables) class

    ;; read relevant precedents into tables and each tables foreign-keys
    ;; into the nodes foreign-key slot. Backtrace-table and f-key-table
    ;; are used for sorting foreign keys based on mutual dependencies.
    (let ((backtrace-table (make-hash-table :test #'eq)))
      (flet ((collate-keys (table-class)
	       (loop
		 for key in (slot-value table-class 'foreign-keys)
		 for f-ref = (getf key :table)
		 for f-key = (list :table f-ref
				   :column (getf key :column))
		 if (gethash f-ref backtrace-table)
		   do (pushnew (class-name table-class) (gethash f-ref backtrace-table))
		 else
		   do (setf (gethash f-ref backtrace-table) (list (class-name table-class))))))
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
      (setf tables (sort-tables tables backtrace-table)))
    (when key-columns
      (loop
	for key-column in key-columns
	do (let ((table (getf key-column :table)))
	     (unless (find-class table)
	       (error "Key column is a plist with keys :TABLE and :COLUMN. The assigned table value is not a table."))
	     (unless (getf key-column :column)
	       (error "There is no column value in the list ~a" key-column))
	     (when tables
	       (unless (member table tables :test #'eq)
		 (error "Key column missing from tables")))
	     (setf tables (cons table (remove table tables :test #'eq))))))))


(defmethod shared-initialize :around ((class db) slot-names
				      &key &allow-other-keys)
  (declare (ignore slot-names))
  (call-next-method)
  (with-slots (schema primary-keys foreign-keys constraints table referenced-columns require-columns) class
    (unless table
      (setf table (db-syntax-prep (class-name class))))
    (loop for column in (filter-slots-by-type class 'db-column-slot-definition)
	  for slot-name = (slot-definition-name column)
	  for to-check = nil

	  ;; primary key 
	  do (with-slots (primary-key column-name foreign-key col-type referenced check) column
	       (setf column-name (db-syntax-prep slot-name)
		     (slot-value column 'table) table)
	       (when primary-key
		 (pushnew column-name primary-keys :test #'string=))
	       (when referenced
		 (pushnew (list column-name col-type) referenced-columns :test #'equal))

	       ;;foreign-key and require-columns
	       (cond (foreign-key
		      (setf (getf foreign-key :key) column-name)
		      (unless (getf foreign-key :schema)
			(setf (getf foreign-key :schema) (schema (find-class (getf foreign-key :table)))))
		      (pushnew foreign-key foreign-keys :test #'equal)
		      (pushnew (list :key slot-name
				     :table (class-name class)
				     :schema (getf foreign-key :schema)
				     :references (getf foreign-key :column)
				     :on-delete (getf foreign-key :on-delete)
				     :on-update (getf foreign-key :on-update))
			       (slot-value (find-class (getf foreign-key :table)) 'referenced-by)
			       :test #'equal))
		     (t
		      (unless (or (eq col-type :serial)
				  (eq col-type :timestamptz))
			(pushnew column require-columns :test #'eq))))
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

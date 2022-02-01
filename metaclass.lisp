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
    :type (null cons)))
  (:documentation "Aggregate tables for transactions, cte's, etc. Map to application objects."))


(defmethod partial-class-base-initargs append ((class db-wrap))
  '(:tables :key-columns))

(defclass db-base-column-definition
  (stw-direct-slot-definition)
  ())

(defclass db-aggregate-slot-definition (db-base-column-definition)
    ((maps-table
      :initarg :maps-table
      :initform (error "Value required for maps-table")
      :type symbol)
     (maps-column
      :initarg :maps-column
      :initform nil
      :type (null symbol)
      :documentation "if neither MAPS-COLUMNS nor MAPS-COLUMN is specified, all columns are mapped.")
     (maps-columns
      :initarg :maps-columns
      :initform nil
      :type (null cons)
      :documentation "if neither MAPS-COLUMNS nor MAPS-COLUMN is specified, all columns are mapped.")
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
   (constraints :initarg :constraints :initform nil :reader constraints :type (null cons))
   (mapped-by :initform nil :reader mapped-by :type (null cons))
   (value-columns :initform nil :type (null string) :reader value-columns)))


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
   (return-on :initarg :return-on :initform nil :type (cons keyword))
   (value :initarg :value :initform nil)
   (mapped-by :initform nil :reader mapped-by)
   (column-name :reader column-name)))


(define-layered-class db-interface-class
 :in-layer db-interface-layer (stw-base-class db-wrap) ())

(define-layered-class db-table-class
 :in-layer db-table-layer (stw-base-class db) ())



;;;;;;; Initialization Methods

(defmethod map-column-p ((slot db-aggregate-slot-definition) (column db-column-slot-definition))
  (with-slots (maps-column maps-columns) slot
    (unless (member (slot-definition-name slot)
		    (mapcar #'(lambda (slot)
				(slot-definition-name slot))
			    (slot-value column 'mapped-by))
		    :test #'eq)
      (let ((name (slot-definition-name column)))
	(or (eq name maps-column)
	    (member name maps-columns))))))


(defmethod default-column-map ((slot db-aggregate-slot-definition))
  (with-slots (maps-columns maps-table) slot
    (setf maps-columns
	  (mapcan #'(lambda (column)
		      (list (slot-definition-name column)))
		  (filter-slots-by-type (find-class maps-table) 'db-column-slot-definition)))))



(defmethod initialize-instance :after ((slot db-aggregate-slot-definition) &key)
  (with-slots (maps-table maps-columns maps-column express-as-type) slot
    (let ((slot-name (slot-definition-name slot)))
      ;; maps table must correspond to a class
      (unless (and maps-table (find-class maps-table))
	(error "the table ~a specified in maps-table of slot ~a does not exist" maps-table (slot-definition-name slot)))

      ;; let the respective table know it is being mapped
      (pushnew slot (slot-value (find-class maps-table) 'mapped-by))

      ;; ensure mapping
      (unless (or maps-column maps-columns)
	(warn "No value set for MAPS-COLUMNS or MAPS-COLUMN for slot ~a. All columns without foreign-keys of table ~a will be mapped."
	      slot-name maps-table)
	(default-column-map slot))

      ;; select statements return type
      (unless express-as-type
	(setf (slot-value slot 'express-as-type) maps-table))

      ;; let the respective columns know they are being mapped also
      (loop for column in (filter-slots-by-type (find-class maps-table) 'db-column-slot-definition)
	 when (map-column-p slot column)
	 do (push slot (slot-value column 'mapped-by))))))



(defmethod shared-initialize :after ((slot db-column-slot-definition) slot-names
				     &key col-type table check primary-key return-on foreign-key &allow-other-keys)
  (declare (ignore slot-names))
  (let ((slot-name (slot-definition-name slot)))

    (when primary-key
      (unless (eq col-type 'serial)
	(setf (slot-value slot 'not-null) t)))

    (when check
      (setf (slot-value slot 'check)
	    (infill-column check slot-name)))

    (flet ((test-return-on (on)
	     (unless (member on '(:insert :update :delete))
	       (error "Return-on must satisfy a condition of :INSERT :UPDATE or :DELETE")))

	   (test-foreign-key (key)
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

      (when return-on
	(typecase return-on
	  (atom (test-return-on return-on))
	  (cons (map nil #'test-return-on return-on))))

      (when foreign-key
	(test-foreign-key foreign-key)
	(pushnew (list :key slot-name
		       :table table
		       :schema (getf foreign-key :schema)
		       :references (getf foreign-key :column)
		       :on-delete (getf foreign-key :on-delete)
		       :on-update (getf foreign-key :on-update))
		 (slot-value (find-class (getf foreign-key :table)) 'referenced-by)
		 :test #'equal)))))




(defmethod shared-initialize :around ((class db-wrap) slot-names
				      &key &allow-other-keys)
  (declare (ignore slot-names))
  (call-next-method)
  (with-slots (key-column tables) class
    (loop for object in (filter-precedents-by-type class 'stw-base-class)
	  do (pushnew (class-name object) tables :test #'eq))
    (when key-column
      (let ((table (getf key-column :table)))
	(unless (find-class table)
	  (error "Key column is a plist with keys :TABLE and :COLUMN. The assigned table value is not a table."))
	(unless (getf key-column :column)
	  (error "There is no column value in the list KEY-COLUMN"))
	(when tables
	  (unless (member table tables :test #'eq)
	    (error "Key column missing from tables")))
	(setf tables (cons table (remove table tables :test #'eq)))))))

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
  (with-slots (primary-keys foreign-keys constraints table) class
    (unless table
      (setf table (db-syntax-prep (class-name class))))
    (loop for column in (filter-slots-by-type class 'db-column-slot-definition)
	  for slot-name = (slot-definition-name column)
	  for to-check = nil
	  do (with-slots (primary-key foreign-key check) column
	       (when primary-key
		 (pushnew (db-syntax-prep slot-name) primary-keys :test #'equal))
	       (when check
		 (setf (getf to-check :check) (slot-value column 'check)
		       (getf to-check :col-name) slot-name
		       (getf to-check :table) table)
		 (pushnew to-check constraints :test #'equal))
	       (when foreign-key
		 (setf (getf foreign-key :key) (db-syntax-prep slot-name))
		 (unless (getf foreign-key :schema)
		   (setf (getf foreign-key :schema) (schema (find-class (getf foreign-key :table)))))
		 (pushnew foreign-key foreign-keys :test #'equal))))
    (when primary-keys
      (setf primary-keys (reverse primary-keys)))))


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

(defmacro define-db-table (name &body body)
  `(define-db-class ,name db-table-layer db-table-class
     ,@body))

(defmacro define-interface-node (name &body body)
  `(define-db-class ,name db-interface-layer db-interface-class
     ,@body))

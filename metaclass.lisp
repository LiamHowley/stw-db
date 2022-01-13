(in-package stw.db)

;;; DB-INTERFACE-LAYER metaclasses

(define-layered-class db-wrap
  :in db-interface-layer (base-class)
  ((tables
    :initarg :tables
    :initform nil
    :accessor tables)
   (key-column
    :initarg :key-column
    :initform nil
    :documentation "Slot of equal value between two instances of the same class. Anchor for updating queries. Typically referenced by multiple columns."
    :reader key-column))
  (:documentation "Aggregate tables for transactions, cte's, etc. Map to application objects."))


(defmethod partial-class-base-initargs append ((class db-wrap))
  '(:tables :key-column))


(defclass db-aggregate-slot-definition (stw-direct-slot-definition)
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
      :documentation "Set requested type for SELECT operations to return. Unless specified, the type
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
  ((schema :initarg :schema :reader schema :type string)
   (table :initarg :table :initform nil :reader table :type string)
   (primary-keys :initarg :primary-keys :initform nil :accessor primary-keys :type (null cons)
   (foreign-keys :initarg :foreign-keys :initform nil :accessor foreign-keys :type (null cons))
   (referenced-by :initarg :referenced-by :initform nil :accessor referenced-by :type (cons null))
   (constraints :initarg :constraints :initform nil :reader constraints :type (null cons))
   (mapped-by :initform nil :reader mapped-by :type (null cons))))


(defmethod partial-class-base-initargs append ((class db))
  '(:schema :table :primary-keys :foreign-keys :constraints))


(defclass db-column-slot-definition (stw-direct-slot-definition)
  ((schema :initform nil :type string)
   (table :initarg :table :initform nil :type symbol)
   (col-type :initarg :col-type :initform :text :reader col-type :type keyword)
   (primary-key :initarg :primary-key :initform nil :type boolean)
   (foreign-key :initarg :foreign-key :initform nil :reader foreign-key :type (cons null))
   (unique :initarg :unique :initform nil :type (boolean null))
   (check :initarg :check :initform nil :type (null cons))
   (default :initarg :default :initform nil)
   (index :initarg :index :initform nil :reader index :type boolean)
   (not-null :initarg :not-null :initform nil :type boolean)
   (return-on :initarg :return-on :initform nil :type (cons keyword))
   (value :initarg :value :initform nil)
   (mapped-by :initform nil :reader mapped-by)
   (column-name :reader column-name)))


(define-layered-class stw-base-class
 :in-layer db-interface-layer (partial-class db-wrap) ())

(define-layered-class stw-base-class
 :in-layer db-table-layer (partial-class db) ())

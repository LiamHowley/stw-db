(in-package stw.db)

(defparameter *schema* "public")

(define-layered-class db-class
  :in db-layer (base-class)
  ())

;;; DB-INTERFACE-LAYER metaclasses

(define-layered-class db-wrap
  :in db-interface-layer (db-class)
  ((schema
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
    (comp-direct-slot-definition)
  ())

(defclass db-aggregate-slot-definition (db-base-column-definition)
  ((maps
    :initarg :maps-table
    :initarg :maps-column
    :initarg :maps-columns
    :reader maps)
   (express-as-type
    :initarg :express-as-type
    :initform :alist
    :type (or null symbol)
    :documentation "Set requested type for SELECT operations to return. Unless otherwise specified, the type
set in maps-table will be returned. If multiple columns are mapped the return values will be contained in a
list and the value EXPRESS-AS-TYPE will apply to the column => value pair. If a single column is mapped the
value EXPRESS-AS-TYPE will refer to the values container.")
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

(defmethod slot-definition-class ((class stw-interface) &key &allow-other-keys)
  'db-aggregate-slot-definition)


;;; DB-TABLE-LAYER metaclasses

(define-layered-class db
  :in db-table-layer (singleton-class db-class)
  ((schema :initarg :schema :reader schema :type string)
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


(defclass enumerated-column-slot-definition (db-column-slot-definition)
  ((store-index :initarg :store-index
                :initform nil
                :reader store-index
                :type boolean
                :documentation "Schemas that conform to a standard may specify an enumerated column
without clearly defining the enumerated values to be stored. In such circumstances storing the index
may make sense, leaving the display / logical / readable values to be determined in the application
layer, thus making the enumerated data portable.")
   (enumerated :initarg :enumerated-values :type array)))


(defmethod slot-definition-class ((class stw-table) &key enumerated-values &allow-other-keys)
  (if enumerated-values
      'enumerated-column-slot-definition
      'db-column-slot-definition))


(define-layered-class db-interface-class
  :in-layer db-interface-layer (comp-base-class db-wrap) ())

(define-layered-class db-table-class
  :in-layer db-table-layer (comp-base-class db) ())

(define-layered-class db-key-table
  :in-layer db-table-layer (db-table-class)
  ()
  (:documentation "Specialised type for tables
with a single column of type serial."))

(define-layered-class db-root-table
  :in-layer db-table-layer (db-table-class)
  ()
  (:documentation "Specialised type for tables containing primary
key column(s) that may be referred to by foreign keys of other tables
but are not themselves foreign keys."))


;;;;;;; Initialization Methods

(defstruct (slot-mapping (:conc-name nil))
  (mapping-node nil :type (or null db-interface-class))
  (mapping-slot nil :type db-aggregate-slot-definition)
  (mapped-table nil :type db-table-class)
  (mapped-column nil :type (or null db-column-slot-definition))
  (mapped-columns () :type list))


(define-layered-method initialize-in-context
  :in db-interface-layer ((slot db-aggregate-slot-definition)
			                    &key maps-table maps-column maps-columns type)
  (with-slots (maps) slot
    (setf (slot-definition-type slot)
	        (or type
	            (when (typep type 'boolean)
		            'list)))
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
					                              collect (find-slot-definition maps-table column 'db-column-slot-definition)))))))


(defclass key ()
  ((table :initarg :table :reader table)
   (ref-schema :initarg :ref-schema :initform nil :reader ref-schema)
   (ref-table :initarg :ref-table :initform nil :reader ref-table)
   (schema :initarg :schema :initform nil :reader schema)
   (on-update :initarg :on-update :initform nil :reader on-update)
   (on-delete :initarg :on-delete :initform nil :reader on-delete)
   (no-join :initarg :no-join :initform nil :type boolean :reader no-join))
  (:documentation "The prefix 'ref-' indicates the referring table, schema."))

(defclass foreign-key (key)
  ((column :initarg :column :reader column)
   (key :initarg :key :initform nil :reader key)))

(defclass composite-key (key)
  ((columns :initarg :column :initform nil :reader columns)
   (keys :initarg :key :initform nil :reader keys)))


(defmethod shared-initialize
    :after ((class key) slot-names &rest initargs &key table column schema ref-schema on-update on-delete)
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
  :in db-table-layer ((slot enumerated-column-slot-definition)
                      &rest rest &key enumerated-values store-index &allow-other-keys)
  (when store-index
    (setf (slot-value slot 'col-type) :smallint
          (getf rest :check) `(<= ,(length enumerated-values))))
  (setf (slot-value slot 'enumerated)
        (make-array (length enumerated-values)
                    :initial-contents enumerated-values
                    :fill-pointer t
                    :adjustable t))
  (apply #'call-next-layered-method slot rest))


(define-layered-method initialize-in-context
  :in db-table-layer ((slot db-column-slot-definition)
                      &key col-type check primary-key foreign-key &allow-other-keys)
  (let ((slot-name (slot-definition-name slot)))
    (ensure-column-type col-type)
    (when (eq col-type :serial)
      (setf (slot-value slot 'lock-value) t))
    (when primary-key
      (unless (eq col-type :serial)
        (setf (slot-value slot 'not-null) t)))
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
  (with-slots (tables) class

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
	             (with-slots (mapping-node mapped-table mapped-column mapped-columns) maps
		             (setf mapping-node class)
		             (pushnew (class-name mapped-table) tables :test #'eq)
		             (collate-keys mapped-table)
		             (pushnew maps (slot-value class 'maps) :test #'eq)
		             (when mapped-column
		               (pushnew maps (slot-value mapped-column 'mapped-by) :test #'eq))
		             (pushnew maps (slot-value mapped-table 'mapped-by) :test #'eq)
		             (loop
		               for column in mapped-columns
		               do (pushnew maps (slot-value column 'mapped-by) :test #'eq)))))
	      (let ((sorted-tables (sort-tables backtrace-table)))
	        (awhen (ensure-bound-tables tables sorted-tables)
	          (warn "the table(s) ~{~a^ ~} are not bound. They either
don't belong in this node or a foreign key is required" self))
          (when sorted-tables
	          (setf tables sorted-tables)))))))


(define-layered-method initialize-in-context
  :in db-table-layer ((class db) &key)
  (with-slots (schema constraints table foreign-keys) class
    (mapcar #'(lambda (slot)
                (slot-makunbound class slot))
            '(primary-keys require-columns))
    (unless table
      (setf table (funcall *reserved-keywords-filter* (db-syntax-prep (class-name class)))))

    ;; foreign-keys
    (unless foreign-keys
      (set-foreign-keys class))

    ;; organise column slots
    (map-filtered-slots
     class
     #'(lambda (slot)
         (typep slot 'db-column-slot-definition))
     #'(lambda (slot)
         (let ((slot-name (slot-definition-name slot))
               (to-check))

	         (with-slots (domain table-class column-name col-type check) slot
	           (setf column-name (funcall *reserved-keywords-filter* (db-syntax-prep slot-name))
		               (slot-value slot 'table) table
		               table-class class
		               domain (funcall *reserved-function/type-filter*
				                           (format nil "~a_~a"
					                                 (db-syntax-prep (class-name class))
					                                 (db-syntax-prep slot-name)))
		               (slot-value slot 'schema) schema)

	           ;; check constraints
	           (when check
	             (setf (getf to-check :check) check
		                 (getf to-check :col-name) slot-name
		                 (getf to-check :table) table)
	             (pushnew to-check constraints :test #'equal))))))))



(define-layered-function set-foreign-keys (class)
  (:method
      :in-layer db-table-layer ((class db))
    (with-slots (foreign-keys) class
        (iterate-extend
          (with= table-functions (make-hash-table :test #'equal)
                 pkeys nil
                 composite-key nil)
          (for slot in (filter-slots-by-type class 'db-column-slot-definition))
          (awhen (slot-value slot 'foreign-key)
            ;; prefix t- indicates the referenced table
            (with-slots (key table ref-table column) self
              (setf ref-table (class-name class))
              (for= table-class (find-class table)

                    ;; A closure is used to allow for the accumulation of composite keys
                    ;; alongside keys that might refer to more than one table.
                    table-function (or (gethash `(process-key-table ,class ,table-class) table-functions)
                                       (setf (gethash `(process-key-table ,class ,table-class) table-functions)
                                             (process-key-table class table-class)))
                    returns (funcall table-function slot self))
              (setf pkeys (car returns)
                    composite-key (cadr returns)
                    foreign-keys (caddr returns))))
          (finally

           ;; Ensure a composite key is not incomplete
           (cond ((and composite-key pkeys)
                  (error "Composite foreign key is not complete and must refer to a unique constraint"))
                 ((and composite-key foreign-keys)
                  (setf foreign-keys `(,@foreign-keys ,composite-key)))
                 (composite-key
                  (setf foreign-keys `(,composite-key)))
                 (foreign-keys
                  (setf foreign-keys `(,@foreign-keys)))))))))



(defmethod process-key-table ((key-table db) (referenced-table db))
  (let* ((pkeys (slot-value referenced-table 'primary-keys))
         (composite-pkey-p (> (length pkeys) 1))
         (key-table-name (table key-table))
         (composite-key (make-instance 'composite-key
                                       :table (class-name referenced-table)
                                       :ref-table (class-name key-table)))
         (foreign-keys (slot-value key-table 'foreign-keys)))
    #'(lambda (key-column foreign-key)
        ;; t- prefix indicates the referenced table.
        (let* ((t-column (find-slot-definition referenced-table (column foreign-key) 'db-column-slot-definition))
               (member-pkeys-p (member t-column pkeys :test #'eq))
               (t-column-col-type (get-column-type t-column))
               (key-col-type (get-column-type key-column)))

          ;;; 1. both t-column and the referring column must agree on type
          (unless (equal key-col-type t-column-col-type)
            (error "The column ~s in table ~s with type ~s references the
column ~s in table ~s with type ~s. Column types must match."
                   (column-name key-column) key-table-name key-col-type
                   (column-name t-column) (table referenced-table) t-column-col-type))

          ;;; 2. foreign key must reference a primary key or a unique key
          (unless (or member-pkeys-p 
                      (slot-value t-column 'unique))
            (error "Foreign key ~s in table ~s must refer to a column with unique constraint."
                   (column-name key-column) key-table-name))

          ;;; 3. Is it a single column foreign key or a composite?
          (cond ((and member-pkeys-p composite-pkey-p)
                 ;; composite key required
                 (with-slots (keys columns) composite-key
                   (push (slot-definition-name t-column) columns)
                   (push (slot-value foreign-key 'key) keys)
                   (setf foreign-keys (remove foreign-key foreign-keys :test #'eq)
                         pkeys (remove t-column pkeys :test #'eq))))
                (t
                 (setf (slot-value foreign-key 'ref-table) (class-name key-table))
                 (pushnew foreign-key foreign-keys :test #'eq))))
        (list pkeys
              (when (columns composite-key)
                composite-key)
              foreign-keys))))


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
               (eq (slot-value slot 'col-type) :serial)
               (slot-boundp slot 'default)
               (null (slot-value slot 'not-null)))
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


(defmacro define-root-table (name &body body)
  `(define-db-class ,name db-table-layer db-root-table
     ,@body))

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

(defmethod slot-unbound (class (instance db-wrap) (slot-name (eql 'schema)))
  (setf (slot-value instance slot-name) *schema*))

(defmethod slot-unbound (class (instance db) (slot-name (eql 'schema)))
  (setf (slot-value instance slot-name) *schema*))

(in-package stw.db)

;;; There are three basic layers. The most primitive is
;;; DB-TABLE-LAYER, which as the name suggests is for
;;; direct mapping with database tables. The next is a
;;; higher level DB-INTERFACE-LAYER which interfaces
;;; with context oriented application objects. Both of
;;; the above inherit from DB-LAYER, which creates an
;;; environment within which it's sub-layers can alternate.

(defclass stw-db-context (stw-layer-context)
  ())

(defclass stw-table (stw-db-context)
  ())

(defclass stw-interface (stw-db-context)
  ())

(deflayer db-layer (stw-base-layer)
  ()
  (:metaclass stw-db-context))



(define-layered-method adjoin-layer-using-class
  :in-layer db-layer ((layer stw-layer-context) active-layers)
  ;; on layer activation deactivate other layers of the same layer type
  (values 
   (call-next-layered-method
    layer
    (remove-layer 'db-layer active-layers))
   t))


;; database operations contexts

(deflayer db-op)

(deflayer insert (db-op)
  ((template-register
    :initarg :register-template
    :initform (make-hash-table :test #'equal)
    :reader template-register)))

(deflayer delete-from (db-op)
  ((template-register
    :initarg :register-template
    :initform (make-hash-table :test #'eq)
    :reader template-register)))

(deflayer update (db-op)
  ((template-register
    :initarg :register-template
    :initform (make-hash-table :test #'equal)
    :reader template-register)))

(deflayer retrieve (db-op)
  ((template-register
    :initarg :register-template
    :initform (make-hash-table :test #'equal)
    :reader template-register)))


;; create interface-node layer contexts for database operations

(deflayer db-interface-layer (db-layer)
  ()
  (:metaclass stw-interface))

(deflayer insert-node (insert db-interface-layer)
  ()
  (:metaclass stw-interface))

(deflayer delete-node (delete-from db-interface-layer)
  ()
  (:metaclass stw-interface))

(deflayer update-node (update db-interface-layer)
  ()
  (:metaclass stw-interface))

(deflayer retrieve-node (retrieve db-interface-layer)
  ()
  (:metaclass stw-interface))


;; create table layer contexts for database operations

(deflayer db-table-layer (db-layer)
  ()
  (:metaclass stw-table))

(deflayer insert-table (insert db-table-layer)
  ()
  (:metaclass stw-table))

(deflayer delete-table (delete-from db-table-layer)
  ()
  (:metaclass stw-table))

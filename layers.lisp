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
  ((direct-slot-class
    :initarg :direct-slot-class
    :initform 'db-column-slot-definition
    :reader direct-slot-class)))

(defclass stw-interface (stw-db-context)
  ((direct-slot-class
    :initarg :direct-slot-class
    :initform 'db-aggregate-slot-definition
    :reader direct-slot-class)))

(deflayer stw-db-layer (stw-base-layer)
  ()
  (:metaclass stw-db-context))

(define-layered-method
    adjoin-layer-using-class
  ((layer stw-db-context) active-layers)
  ;; on layer activation deactivate other layers of the same layer type
  (values 
   (call-next-layered-method layer
    (remove-layer 'stw-db-layer active-layers))
   t))


(deflayer db-layer (stw-db-layer)
  ((conn
    :initarg :conn
    :initform nil
    :accessor conn
    :special t))
  (:metaclass stw-db-context))

(deflayer db-table-layer (db-layer)
  ()
  (:metaclass stw-table))

(deflayer db-interface-layer (db-layer)
  ()
  (:metaclass stw-interface))

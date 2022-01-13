(in-package :cl-user)

(defpackage :stw.db
  (:use :cl)
  (:import-from :stw.meta
		:stw-base-layer
		:stw-layer-context
		:direct-slot-class)
  (:import-from :contextl
		:deflayer
		:define-layered-method
		:call-next-layered-method
		:remove-layer
		:adjoin-layer-using-class))

(in-package :stw.db)

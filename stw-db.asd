(defsystem "stw-db"
    :depends-on ("stw-meta"
		 "stw-utils"
		 "uuid"
		 "cl-postgres"
		 "bordeaux-threads"
		 "atomics"
		 "local-time"
		 "cl-postgres+local-time")
    :description ""
    :serial t
    :components ((:file "package")
		 (:file "conditions")
		 (:file "util")
		 (:file "layers")
		 (:file "keywords")
		 (:file "metaclass")
		 (:file "connect")
		 (:file "setting-up")
		 (:file "tearing-down")
		 (:file "procedure")
		 (:file "function")
		 (:file "dispatch")
		 (:file "select")
		 (:file "insert")
		 (:file "delete")
		 (:file "update"))
    :long-description
    #.(uiop:read-file-string
       (uiop:subpathname *load-pathname* "docs/README.org"))
    :in-order-to ((test-op (load-op :stw-db-test))))

(defsystem "stw-db"
    :depends-on ("stw-meta"
		 "stw-utils"
		 "cl-postgres"
		 "bordeaux-threads"
		 "atomics")
    :description ""
    :serial t
    :components ((:file "package")
		 (:file "util")
		 (:file "layers")
		 (:file "metaclass")
		 (:file "connect")
		 (:file "setting-up")
		 (:file "tearing-down")
		 (:file "procedure")
		 (:file "function")
		 (:file "dispatch")
		 (:file "insert")
		 (:file "delete")
		 (:file "update")
		 (:file "select"))
    :long-description
    #.(uiop:read-file-string
       (uiop:subpathname *load-pathname* "README.txt"))
    :in-order-to ((test-op (load-op :stw-db-test))))

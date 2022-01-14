(defsystem "stw-db"
    :depends-on ("stw-meta"
		 "stw-utils")
    :description ""
    :serial t
    :components ((:file "package")
		 (:file "util")
		 (:file "layers")
		 (:file "metaclass")
		 (:file "statements"))
    :long-description
    #.(uiop:read-file-string
       (uiop:subpathname *load-pathname* "README.txt"))
    :in-order-to ((test-op (load-op :stw-db-test))))

(defsystem "stw-db"
  :author "Liam Howley <liam.howley@thespanningtreeweb.ie>"
  :license "MIT"
    :depends-on ("cl-comp"
		 "stw-utils"
		 "uuid"
		 "cl-postgres"
		 "bordeaux-threads"
		 "atomics"
		 "local-time"
		 "cl-postgres+local-time")
    :description "A Context Oriented Object Relational Model for PostgreSQL, with a focus on cacheable procedures and functions."
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

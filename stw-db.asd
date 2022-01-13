(defsystem "stw-db"
    :depends-on ("stw-meta"
		 "stw-utils")
    :description ""
    :components ((:file "package")
		 (:file "layers")
		 (:file "metaclass")
		 (:file "statements")))

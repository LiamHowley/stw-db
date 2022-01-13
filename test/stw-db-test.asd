(defsystem #:stw-db-test
    :description "Test suite for stw-db."
    :depends-on ("stw-db" "parachute")
    :serial t
    :components ((:file "package")
		 (:file "stw-db"))
    :perform (asdf:test-op (op c) (uiop:symbol-call :parachute :test :stw.db.test)))

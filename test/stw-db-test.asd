(defsystem #:stw-db-test
    :description "Test suite for stw-db."
    :depends-on ("parachute" "stw-meta" "stw-db" "contextl")
    :serial t
    :components ((:file "package")
		 (:file "util")
		 (:file "db"))
    :perform (asdf:test-op (op c) (uiop:symbol-call :parachute :test :stw.db.test)))

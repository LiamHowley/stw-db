(defsystem #:stw-db-test
  :description "Test suite for stw-db."
  :depends-on ("cl-postgres" "parachute" "stw-meta" "stw-db" "contextl")
  :serial t
  :components ((:file "package")
	       (:file "connect")
	       (:file "util")
	       (:file "db")
	       (:file "query"))
  :perform (asdf:test-op (op c) (uiop:symbol-call :parachute :test :stw.db.test)))

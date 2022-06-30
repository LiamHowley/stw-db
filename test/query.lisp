(in-package stw.db.test)


(defparameter *new-account*
  (make-instance 'account
		 :url "foo.com"
		 :name "foo"
		 :password "12345abc"
		 :created-by 1
		 :email "foo@foobar.com"
		 :sites '("foo.com" "bar.com" "baz.com")))


(define-test setting-up...
  :parent stw-db
  :depends-on (connecting...)
  (db-connect db (insert-node)
    (execute *account* nil :optional-join '(user-handle)))
  (true (slot-boundp *account* 'id)))


(define-test tearing-down...
  :parent stw-db
  :depends-on (setting-up...)
  (db-connect db (db-layer)
    (drop-schema *schema* t)))

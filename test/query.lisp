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
  :parent live-test
  :depends-on (connecting...)
  (db-connect db (insert-node)
    (sync *new-account* nil))
  (true (slot-boundp *new-account* 'id))
  (of-type timestamp (slot-value *new-account* 'created-on)))


(define-test updating...
  :parent live-test
  :depends-on (setting-up...)
  (db-connect db (update-node)
    (let ((copy (clone-object *new-account*)))
      (with-slots (email url handle name password sites) copy
	(setf email "baz@foobar.com"
	      name "baz"
	      url nil
	      handle "anonymous"
	      password "foo,bar.123$abc"
	      sites '("foo.com" "baz.ie")))
      (sync *new-account* copy)))
  (with-slots (email url handle name password sites) *new-account*
    (is string= "baz@foobar.com" email)
    (is string= "baz" name)
    (is equal nil url)
    (is string= "anonymous" handle)
    (is string= "foo,bar.123$abc" password)
    (is equal '("baz.ie" "foo.com") sites)))


(define-test retrieving-data
  :parent live-test
  :depends-on (updating...)
  (let ((account (make-instance 'account :name "baz"))
	(new-account (make-instance 'account :name "baz" :email "baz")))
    (db-connect db (retrieve-node)
      (is eql (sync account nil) nil)
      (of-type account (sync account nil :optional-join '(user-url)))
      (of-type account (sync new-account nil :optional-join '(user-url) :union-queries '(user-name user-email)))
      (is string= (slot-value new-account 'email) "baz@foobar.com")
      (is string= (slot-value *new-account* 'email) (slot-value account 'email))
      (is string= (slot-value *new-account* 'email) (slot-value new-account 'email)))))

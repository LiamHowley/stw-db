(in-package stw.db.test)


(defparameter *new-account*
  (make-instance 'account
		 :url "foo.com"
		 :name "foo"
		 :password "12345abc"
		 :created-by 1
		 :emails '("foo@foobar.com" "foo@bar.com")
		 :sites `(((site . "foo.com") (ip . "123.345.234.1"))
			  ((site . "bar.com") (ip . "234.987.1.1"))
			  ((site . "baz.com") (ip . "234.234.234.2")))))

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
      (with-slots (emails url handle name password sites) copy
	(setf emails '("baz@foobar.com" "foo@bar.com")
	      name "baz"
	      url nil
	      handle "anonymous"
	      password "foo,bar.123$abc"
	      sites `(((site . "foo.com") (ip . "124.345.234.1"))
		      ((site . "bar.com") (ip . "234.987.1.1"))
		      ((site . "foobar.com") (ip . "234.234.234.2")))))
      (sync *new-account* copy)))
  (with-slots (emails url handle name password sites) *new-account*
    (is equal '("baz@foobar.com" "foo@bar.com") emails)
    (is string= "baz" name)
    (is equal nil url)
    (is string= "anonymous" handle)
    (is string= "foo,bar.123$abc" password)
    (is equal `(((site . "foo.com") (ip . "124.345.234.1"))
		((site . "foobar.com") (ip . "234.234.234.2"))
		((site . "bar.com") (ip . "234.987.1.1")))
	sites)))


(define-test retrieving-data
  :parent live-test
  :depends-on (updating...)
  (let ((account (make-instance 'account :name "baz"))
	(new-account (make-instance 'account :name "baz" :emails '("baz"))))
    (db-connect db (retrieve-node)
      (is eql (sync account nil) nil)
      (of-type account (sync account nil :optional-join '(user-url)))
      (of-type account (sync new-account nil :optional-join '(user-url) :union-queries '(user-name user-email)))
      (false (set-difference '("baz@foobar.com" "foo@bar.com") (slot-value new-account 'emails) :test #'string=))
      (false (set-difference (slot-value account 'emails) (slot-value *new-account* 'emails) :test #'string=))
      (false (set-difference (slot-value new-account 'emails) (slot-value *new-account* 'emails) :test #'string=)))))

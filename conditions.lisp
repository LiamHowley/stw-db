(in-package stw.db)

(define-condition invalid-operator-error (simple-error)
  ())

(defun invalid-operator-error (format-control &rest format-args)
  (error 'invalid-operator-error
	 :format-control format-control
	 :format-arguments format-args))


(define-condition null-key-error (simple-error)
  ((slot-name :initarg :slot-name :reader slot-name)
   (object :initarg :object :reader object))
  (:report (lambda (c s)
	     (format s "Null value for key ~s in class ~s. May affect more than one record."
		     (slot-name c) (class-name (class-of (object c))))))
  (:documentation "When updating/deleting, root table primary key values are essential in order 
to keep the operation to specific database tuples. Without a root table key the likelihood
of an update affecting more than one tuple is high."))

(defun null-key-error (slot-name object)
  (error 'null-key-error
	 :slot-name slot-name
	 :object object))

(defun not-an-error ()
  "NULL-KEY-ERROR is invoked when a root table key is either unbound or assigned null value.
It is not always an error however. In specific contexts it may result in a delete operation, or
an update, on tables not subject to a cascade.

Bear in mind that all other slots, with or without values, will determine the nature of the 
query."
  (let ((restart (find-restart 'not-an-error)))
    (when restart (invoke-restart restart))))


(define-condition update-key-value-error (simple-error)
  ((received-value :initarg :received-value :reader received-value)
   (expected-value :initarg :expected-value :reader expected-value))
  (:report
   (lambda (c s) 
     (format s "Expected value: ~a. Received value: ~a." (expected-value c) (received-value c))))
  (:documentation "On copying an interface node object: If the original node 
has a root table primary key value while the clone has either no value, is unbound, 
or a value that does not match the original node, a correctable UPDATE-KEY-VALUE-ERROR is invoked, 
and the EXPECTED-VALUE is provided. See USE-EXPECTED-VALUE restart for options and notes
on handling this error. In the event that an update of values is desired, select CONTINUE."))

(defun update-key-value-error (expected-value actual-value)
  (cerror "Mismatch between root table keys during update operation. Continue anyway?"
	  'update-key-value-error
	  :expected-value expected-value
	  :received-value actual-value))

(defun use-expected-value ()
  "IMPORTANT: The purpose of providing a USE-EXPECTED-VALUE restart is to update varied objects of the
same class type but with fixed values and under specific circumstances, (e.g. updating from a template 
or resetting to some default state). As an error is called and the restart invoked, the error value is 
amended to the slot EXPECTED-VALUE so that the process of updating the node can continue as before.  
However, unless the respective key values are expected to differ, this restart should be approached with 
caution. The root keys of a node should match. That they do not match must be deliberate and should not 
occur carelessly."
  (let ((restart (find-restart 'use-expected-value)))
    (when restart (invoke-restart restart))))

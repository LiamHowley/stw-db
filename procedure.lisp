(in-package stw.db)


(define-layered-class procedure
  :in db-layer ()
  ((name :initarg :name :accessor name)
   (args :initarg :args :initform nil :accessor args)
   (p-values :initarg :values :initform nil :accessor p-values)
   (vars :initarg :vars :initform nil :accessor vars)
   (statements :initarg :statements :accessor statements)))

(define-layered-method statement
  :in-layer db-interface-layer ((class procedure))
  (with-slots (name args vars statements) class
    (format nil "CREATE OR REPLACE PROCEDURE ~a (~a)~%LANGUAGE plpgsql~%AS $BODY$~%~a~%BEGIN~%~a~%END;~%$BODY$;"
	    name 
	    (format nil "~@[~{~{~a~@[ ~a~]~}~^, ~}~]" args)
	    (format nil "~@[~{~{DECLARE ~a ~a~@[ := ~a~];~}~%~}~]" vars)
	    (format nil "~{~a~%~}" statements))))

(define-layered-function call (procedure)
  (:method
      :in db-layer ((class procedure))
    (with-slots (name p-values) class
      (format nil "CALL ~a (~@[~{~a~^, ~}~])" name p-values))))



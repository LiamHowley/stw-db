(in-package stw.db)


(defgeneric db-syntax-prep (this)
  (:method ((this string))
    (string-downcase
     (substitute #\_ #\- this)))
  (:method ((this symbol))
    (string-downcase
     (substitute #\_ #\- (symbol-name this))))
  (:documentation "returns string with - replaced with _"))


(defun set-sql-name (&rest rest)
  (format nil "~{~a~^.~}"
	  (mapcan #'(lambda (item)
		      (list (db-syntax-prep item)))
		  rest)))


(defun date/time-p (input)
  (typecase input
    (symbol
     (date/time-p (symbol-name input)))
    (string
     (some #'identity
	   (mapcan #'(lambda (col-type)
		       (let ((mismatch (mismatch (string-downcase input) col-type)))
			 (if mismatch
			     (list (eql mismatch (length col-type)))
			     (list t))))
		   '("timestamp" "date" "time" "interval"))))))


(declaim (inline sql-op))

(defun sql-op (op)
  (declare (optimize (speed 3) (safety 0)))
  (setf op
	(case op
	  ((char-equal
	    char=
	    string-equal
	    string=
	    equalp
	    equal
	    eql
	    eq)
	   :=)
	  ((char-not-equal
	    char/=
	    string-not-equal
	    string/=
	    /=)
	   :<>)
	  ((char<
	    char-lessp
	    string<
	    string-lessp)
	   :<)
	  ((char>
	    char-greaterp
	    string>
	    string-greaterp)
	   :>)
	  ((char<=
	    char-not-greaterp
	    string<=
	    string-not-greaterp)
	   :<=)
	  ((char>=
	    char-not-lessp
	    string>=
	    string-not-lessp)
	   :>=)
	  (t op))))


(defun infill-column (list column)
  (let ((op (car list)))
    (when (and (member op '(= > < /= >= <=))
	       (eql (list-length list) 2))
      (push column (cdr list)))
    (labels ((walk (inner acc)
	       (if (null inner)
		   (nreverse acc)
		   (walk (cdr inner)
			 (typecase (car inner)
			   (atom
			    (cons (car inner) acc))
			   (cons 
			    (cons (infill-column (car inner) column) acc)))))))
      (walk list nil))))

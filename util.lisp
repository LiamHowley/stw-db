(in-package stw.db)


(defun as-prefix (word)
  (string-trim '(#\") word))


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


(declaim (inline boolean-value))

(defun boolean-value (value)
  (if (numberp value)
      (case integer
	(1 (values t t))
	(0 (values t nil))
	(t (values nil nil)))
      (scase value
	     (("t" :t t)
	      (values t t))
	     (("f" :f nil)
	      (values t nil))
	     (t (values nil nil)))))

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


(defun infix-constraint (list &optional column)
  "Infixing is done in the process of creating a constraint. COLUMN is optional
but must be provided if not already encoded within LIST."
  (let ((op (sql-op (car list))))
    (cond ((consp op)
	   (invalid-operator-error "~a is not a valid operator." (car list)))
	  ((eql (list-length list) 2)
	   (push (db-syntax-prep column) (cdr list))))
    (labels ((walk (inner acc)
	       (if (null inner)
		   (format nil "~a" (nreverse (butlast acc)))
		   (walk (cdr inner)
			 (typecase (car inner)
			   (string
			    (cons (if (equal (car inner) (car (last inner)))
				      (format nil "'~a'" (car inner))
				      (format nil "~a" (car inner)))
				  (push op acc)))
			   (atom
			    (cons (car inner) (push op acc)))
			   (cons 
			    (cons (infix-constraint (car inner) column) (push op acc))))))))
      (walk (cdr list) nil))))

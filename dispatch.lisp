(in-package stw.db)


(define-layered-function generate-component (class))

(define-layered-function make-sql-statement (class procedure &optional tables))


(define-layered-function read-row-to-class (class)

  (:method
      :in db-layer ((class serialize))
    (flet ((get-symbol-name (field)
	     (string-upcase (substitute #\- #\_ (string-left-trim '(#\_) field)))))
      (row-reader (fields)
	(let ((base-class (class-of class)))
	  (loop
	    while (next-row)
	    do (loop
		 for field across fields
		 for symbol-name = (get-symbol-name (field-name field))
		 for slot-name = (intern symbol-name (symbol-package (class-name base-class)))
		 when (find-slot-definition base-class slot-name 'db-column-slot-definition)
		   do (setf (slot-value class slot-name) (next-field field)))))))))


(define-layered-function execute (class component)

  (:method 
      :in-layer db-layer :around ((class serialize) component)
    (multiple-value-bind (statement procedure)
	(call-next-method)
      (handler-case (exec-query *db* statement (read-row-to-class class))
	(database-error (err)
	  (error-handler class procedure err component))))
    class)

  (:method
      :in-layer db-layer ((class serialize) component)
    (let* ((base-class (class-of class))
	   (schema (slot-value base-class 'schema))
	   (procedure (make-instance 'procedure :schema schema)))
      (values
       (dispatch-statement class procedure)
       procedure)))

  (:method
      :in-layer db-layer ((class serialize) (component db-table-class))
    (let ((procedure (memoized-funcall #'generate-component component)))
      (values
       (dispatch-statement class procedure)
       procedure))))



(define-layered-function dispatch-statement (class procedure)

  (:method
      :in-layer db-layer :around ((class serialize) (procedure procedure))
    (with-slots (name schema p-values) procedure
      (setf schema (slot-value (class-of class) 'schema)
	    p-values (call-next-layered-method))
      (call-statement procedure)))

  (:method
      :in-layer db-table-layer ((class serialize) (procedure procedure))
    (with-slots (p-controls table) procedure
      (loop
	with mapped-by = (slot-value table 'mapped-by)
	for control in p-controls
	if (car control)
	  collect (process-values class control mapped-by)
	else
	  collect (let* ((slot (cadr control))
			 (slot-name (slot-definition-name slot))
			 (value (prepare-value slot (when (slot-boundp class slot-name)
						      (slot-value class slot-name)))))
		    (if value value (error "Value missing for slot ~s" slot-name)))))))


(define-layered-function process-values (class controls mapped
					       &optional parenthesize)
  (:documentation "Values are collated, prepared and passed as args
to be formatted. Mapped slots are refer to the mapping slot for value
acquisition. Returns pg array string.")

  (:method
      :in-layer db-layer ((class serialize) (controls cons) mapped
			  &optional parenthesize)
    (destructuring-bind (control slots) controls
      (setf slots (ensure-list slots))
      (apply #'format nil control
	     (if mapped
		 ;; one to many 
		 (loop
		   for mapping in mapped
		   for mapping-node = (mapping-node mapping)
		   for mapping-slot-name = (slot-definition-name (mapping-slot mapping))
		   when (and (eq (class-of class) mapping-node)
			     (slot-boundp class mapping-slot-name))
		     collect (loop for value in (slot-value class mapping-slot-name)
				   collect (prepare-value (mapped-column mapping) value parenthesize)))
		 ;; one to one 
		 (loop
		   for slot in slots
		   for slot-name = (slot-definition-name slot)
		   collect (prepare-value slot (slot-value class slot-name) parenthesize)))))))


(define-layered-function prepare-value (slot value &optional parenthesize)
  (:method
    :in db-layer ((slot db-column-slot-definition) value &optional parenthesize)
    (with-slots (col-type default not-null) slot
      (cond (value
	     (case col-type
	       (:boolean
		(if (eq value t) "'t'" "'f'"))
	       ((:text :varchar)
		(if parenthesize
		    (concatenate 'string "'(" value ")'")
		    (concatenate 'string "'" value "'")))
	       (t
		value)))
	    ((and not-null default (eq col-type :boolean))
	     "'f'")
	    (default
	     "null")))))

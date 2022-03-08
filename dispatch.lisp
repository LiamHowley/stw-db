(in-package stw.db)


(define-layered-function dispatch-statement (class procedure)

  (:method
      :in-layer db-table-layer ((class serialize) (procedure procedure))
    (with-slots (p-values p-controls table) procedure
      (setf schema (slot-value (class-of class) 'schema)
	    p-values (loop
		       with mapped-by = (slot-value table 'mapped-by)
		       for control in p-controls
		       if (car control)
			 collect (process-values class control mapped-by)
		       else
			 collect (let ((slot (cadr control)))
				   (prepare-value (slot-value slot 'col-type)
						  (slot-value class (slot-definition-name slot))))))
      (call-statement procedure))))


(define-layered-function process-values (class controls mapped)
  (:documentation "Values are collated, prepared and passed as args
to be formatted. Mapped slots are refer to the mapping slot for value
acquisition. Returns pg array string.")

  (:method
      :in-layer db-layer ((class serialize) (controls cons) mapped)
    (destructuring-bind (control slots) controls
      (setf slots (ensure-list slots))
      (let ((parenthesize (when (eql (length slots) 1) t)))
	(apply #'format nil control
	       (if mapped
		   ;; one to many 
		   (loop
		     for mapping in mapped
		     for mapping-node = (mapping-node mapping)
		     for mapping-slot-name = (slot-definition-name (mapping-slot mapping))
		     when (eq (class-of class) mapping-node)
		       collect (loop for value in (slot-value class mapping-slot-name)
				     collect (prepare-value (slot-value (mapped-column mapping) 'col-type)
							    value
							    parenthesize)))
		   ;; one to one 
		   (loop
		     for slot in slots
		     for slot-name = (slot-definition-name slot)
		     collect (prepare-value (slot-value slot 'col-type)
					    (slot-value class slot-name) parenthesize))))))))


(defun prepare-value (col-type value &optional parenthesize)
  (case col-type
    (:boolean
     (if (eq value t) "'t'" "'f'"))
    ((:text :varchar)
     (if parenthesize
	 (concatenate 'string "'(" value ")'")
	 (concatenate 'string "'" value "'")))
    (t
     value)))

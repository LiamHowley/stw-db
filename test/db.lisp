(in-package stw.db.test)

;;;; setting up

(defmacro define-db-table (name &body body)
  `(stw.db:define-db-table ,name
     ,@body
     (:schema . ,*schema*)))

(defmacro define-key-table (name &body body)
  `(stw.db:define-key-table ,name
     ,@body
     (:schema . ,*schema*)))


(define-key-table user-base ()
  ((id :col-type :serial
       :root-key t
       :referenced t)))


(define-db-table user-account ()
  ((id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (password :col-type :text
	     :not-null t)
   (created-on :col-type :timestamptz
	       :default (now))
   (created-by :col-type :integer
	       :not-null t
	       :foreign-key (:table user-base
			     :column id
			     :on-delete :cascade
			     :on-update :cascade
			     :no-join t))
   (validated :col-type :boolean
	      :default nil)))


(define-db-table user-id ()
  ((user-id :col-type :serial
	    :primary-key t
	    :referenced t)
   (id :col-type :integer
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))))


(define-db-table user-email ()
  ((email :col-type :text
	  :not-null t
	  :primary-key t)
   (user-id :col-type :integer
	    :not-null t
	    :foreign-key (:table user-id
			  :column user-id
			  :on-delete :cascade
			  :on-update :cascade))
   (id :col-type :integer
       :not-null t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))))


(define-interface-node user (user-base user-id user-email)
  ())


(define-db-table user-name ()
  ((id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (name :col-type :text
	 :index t
	 :not-null t)))


(define-db-table user-site ()
  ((id :col-type :integer
       :primary-key t
       :not-null t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (site :primary-key t
	 :not-null t
	 :col-type :text)))


(define-db-table user-handle ()
  ((handle :col-type :text
	   :not-null t
	   :primary-key t)
   (id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))))


(define-db-table user-url ()
  ((url :col-type :text
	:primary-key t)
   (id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))))


(define-db-table validate ()
  ((vkey :col-type :serial
	 :primary-key t
	 :referenced t)
   (token :col-type :text
	  :not-null t)
   (expires-on :col-type :timestamp)))


(define-db-table user-validate ()
  ((vkey :col-type :integer
	 :primary-key t
	 :foreign-key (:table validate
		       :column vkey
		       :on-delete :cascade
		       :on-update :cascade))
   (id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (purpose :col-type :text
	    :not-null t
	    :check (or (/= "alive") (= "dead")))))



(define-interface-node account
  (user user-account user-name user-handle user-url)
  ((sites :maps-table user-site :maps-column site :type list)))



;;;;; tests

(define-test setting-up...
  :parent stw-db)


(define-test check-schema
  :parent setting-up...
  (with-active-layers (db-layer)
    (is string=
	"CREATE SCHEMA IF NOT EXISTS stw"
	(create-schema *schema*))
    (is string=
	"SET search_path TO stw, public"
	(set-schema *schema*))
    (is string=
	"GRANT ALL PRIVILEGES ON SCHEMA stw TO liam"
	(set-privileged-user *schema* "liam"))))


(define-test creating-table...etc
  :parent setting-up...
  (with-active-layers (db-table-layer)
    (is string=
	"CREATE TABLE IF NOT EXISTS stw.user_account (id INTEGER NOT NULL, password TEXT NOT NULL, created_on TIMESTAMPTZ DEFAULT NOW(), created_by INTEGER NOT NULL, validated BOOLEAN DEFAULT 'f', PRIMARY KEY (id));"
	(create-statement (find-class 'user-account)))

    (is equal
	'("IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stw_user_account_created_by_fkey') THEN ALTER TABLE stw.user_account ADD CONSTRAINT stw_user_account_created_by_fkey FOREIGN KEY (created_by) REFERENCES stw.user_base (id) ON UPDATE CASCADE ON DELETE CASCADE; end if;"
	  "IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stw_user_account_id_fkey') THEN ALTER TABLE stw.user_account ADD CONSTRAINT stw_user_account_id_fkey FOREIGN KEY (id) REFERENCES stw.user_base (id) ON UPDATE CASCADE ON DELETE CASCADE; end if;")
	(foreign-keys-statements (find-class 'user-account)))

    ;; don't know why but this always fails. The expected result is correct
    (is equal 
	'("CREATE INDEX IF NOT EXISTS stw_user_email_user_id_idx ON stw.user_email (user_id);"
	  "CREATE INDEX IF NOT EXISTS stw_user_email_id_idx ON stw.user_email (id);")
	(index-statement (find-class 'user-email)))

    ;; only produces a statement for classes with tables that have a non-indexed foreign key.
    ;; Primary keys are indexed by default.
    (false (index-statement (find-class 'validate)))))


(defvar *user*) 
(defvar *account*)

(define-test table-order...
  :parent stw-db
  (with-active-layers (insert-node)

    (let ((*account* (make-instance 'account :name "liam" :email "foo@bar.com"
					     :password "asdfasdf" :url "foobar.com"
					     :created-by 1 :validated t)))
      (is equal
	  `(user-base user-url user-name user-account user-id user-email)
	  (with-active-layers (insert-node)
	    (stw.db::include-tables *account*)))
      (true (stw.db::slot-to-go *account* (stw.meta:find-slot-definition (find-class 'user-email) 'email)))
      (false (stw.db::slot-to-go *account* (stw.meta:find-slot-definition (find-class 'user-handle) 'handle)))

      (setf (slot-value *account* 'handle) "foo"
	    (slot-value *account* 'email) nil)

      (is equal
	  `(user-base user-url user-handle user-name user-account user-id)
	  (with-active-layers (insert-node)
	    (stw.db::include-tables *account*)))
      (false (stw.db::slot-to-go *account* (stw.meta:find-slot-definition (find-class 'user-email) 'email)))
      (true (stw.db::slot-to-go *account* (stw.meta:find-slot-definition (find-class 'user-handle) 'handle))))))


(define-test mapping...
  :parent stw-db
  (let ((*account* (make-instance 'account :name "liam" :email "foo@bar.com"
					   :password "asdfasdf" :url "foobar.com"
					   :created-by 1 :validated t)))
    (with-active-layers (db-layer)
      (of-type stw.db::slot-mapping (stw.db::match-mapping-node (class-of *account*) (find-class 'user-site)))
      (let ((slot (find-slot-definition (find-class 'user-site) 'site 'db-column-slot-definition)))
	(of-type 'stw.db::slot-mapping (stw.db::match-mapping-node (class-of *account*) slot))))))


(define-test dispatching...
  :parent stw-db 
  :depends-on (table-order...)

  (let ((*account* (make-instance 'account :name "liam" :email "foo@bar.com"
					   :password "asdfasdf" :url "foobar.com"
					   :created-by 1 :validated t))
	(*user* (make-instance 'user :email "liam@foobar.com")))

    (setf (slot-value *account* 'handle) "foo"
	  (slot-value *account* 'email) nil)

    (with-active-layers (db-interface-layer)

      (let ((format-components (stw.db::sql-typed-array (find-class 'user-account))))
	(is string= "ARRAY[(~a, ~a, ~a)]::stw.user_account_type[]" (car format-components))
	(is eql 3 (length (cadr format-components)))
	(loop
	  for slot in (cadr format-components)
	  do (of-type 'db-column-slot-definition slot))

	(let ((foo-slot-definition (find-slot-definition (class-of *account*) 'handle 'db-column-slot-definition)))
	  (is string= "'(foo)'" (stw.db::prepare-value% foo-slot-definition (slot-value *account* 'handle) t))
	  (is string= "'foo'" (stw.db::prepare-value% foo-slot-definition (slot-value *account* 'handle)))))

      (with-active-layers (insert-node)
	(let ((procedure (generate-procedure *user* nil)))
	  (of-type stw.db::procedure procedure)
	  (is equal
	      '((:OUT "_id" :INTEGER) (:OUT "_user_id" :INTEGER)
		(:IN "stw.user_email_type[]" NIL))
	      (slot-value procedure 'stw.db::args))
	  (is string= "CALL stw.user_insert (null, null, ARRAY[(~a)]::stw.user_email_type[])"
	      (slot-value procedure 'stw.db::p-control))
	  (is string= "CALL stw.user_insert (null, null, ARRAY[('(liam@foobar.com)')]::stw.user_email_type[])"
	      (dispatch-statement *user* procedure))))

      (setf (slot-value *account* 'email) "foo@bar.com")

      (let ((format-components (stw.db::sql-typed-array (find-class 'user-email))))
	(is string= "ARRAY[(~a)]::stw.user_email_type[]" (car format-components))
	(is eql 1 (length (cadr format-components)))
	(of-type 'db-column-slot-definition (caadr format-components)))

      (with-active-layers (update-node)
	(let ((clone (clone-object *account*)))
	  (setf (slot-value clone 'email) "bar@foo.com")
	  (fail (generate-procedure *account* clone) 'unbound-slot "The root-key 'ID is unbound")
	  (setf (slot-value *account* 'id) 1
		(slot-value clone 'id) 2)
	  (fail (generate-procedure *account* clone) 'error "The root-key slots 'ID do not match.")
	  (setf (slot-value clone 'id) 1)
	  (let ((procedure (generate-procedure *account* clone)))
	    (of-type stw.db::procedure procedure)
	    (is equal
		'((:INOUT "set_email" "stw.user_email_email")
		  (:IN "where_email" "stw.user_email_email")
		  (:IN "where_id" "stw.user_email_id"))
		(slot-value procedure 'stw.db::args))
	    (is string=
		"CALL stw.account_update (~a::stw.user_email_email, ~a::stw.user_email_email, ~a::stw.user_email_id)"
		(slot-value procedure 'stw.db::p-control))
	    (is string=
		"CALL stw.account_update ('bar@foo.com'::stw.user_email_email, 'foo@bar.com'::stw.user_email_email, 1::stw.user_email_id)"
		(update-op-dispatch-statement *account* clone procedure)))))
      
      (let ((format-components (stw.db::sql-typed-array (find-class 'user-site))))
	(is string= "ARRAY[~{(~a)~^, ~}]::stw.user_site_type[]" (car format-components))

	(with-active-layers (insert-table)
	  (setf (slot-value *account* 'sites) '("foo.com" "bar.com" "baz.com"))
	  (let ((procedure (generate-procedure *account* (find-class 'user-site))))
	    (of-type stw.db::procedure procedure)
	    (is string=
		"CALL stw.user_site_insert (~a, ARRAY[~{(~a)~^, ~}]::stw.user_site_type[])"
		(slot-value procedure 'stw.db::p-control))
	    (is equal
		'((:IN "insert_id" :INTEGER) (:INOUT "insert_sites" "stw.user_site_type[]"))
		(slot-value procedure 'stw.db::args))
	    (is string=
		"CALL stw.user_site_insert (1, ARRAY[('(foo.com)'), ('(bar.com)'), ('(baz.com)')]::stw.user_site_type[])"
		(dispatch-statement *account* procedure))))
	
	(with-active-layers (delete-table)
	  (let ((procedure (generate-procedure *account* (find-class 'user-site))))
	    (of-type stw.db::procedure procedure)
	    (is string=
		"CALL stw.user_site_delete (~a, ARRAY[~{~a~^, ~}])"
		(slot-value procedure 'stw.db::p-control))
	    (is equal
		'((:IN "_id" :INTEGER) (:INOUT "delete_sites" "TEXT[]"))
		(slot-value procedure 'stw.db::args))
	    (is string=
		"CALL stw.user_site_delete (1, ARRAY['(foo.com)', '(bar.com)', '(baz.com)'])"
		(dispatch-statement *account* procedure))))))))



(define-test retrieving...
  :parent stw-db
  (with-active-layers (retrieve-node)
    (let* ((*user* (make-instance 'user :email "liam@foobar.com"))
	   (function (generate-procedure *user* nil)))
      (of-type 'stw.db::db-function function)
      (is equal
	  '((:IN "_email" "stw.user_email_email"))
	  (slot-value function 'stw.db::args))
      (is string=
	  "user_retrievec3$1$$2$$3$$4$"
	  (slot-value function 'stw.db::name))
      (is string=
	  "SELECT * FROM stw.user_retrievec3$1$$2$$3$$4$ (~a::stw.user_email_email)"
	  (slot-value function 'stw.db::p-control))
      (is string=
	  "SELECT stw.user_base.id, stw.user_id.user_id, stw.user_email.email FROM stw.user_base INNER JOIN stw.user_id ON (stw.user_base.id = stw.user_id.id) INNER JOIN stw.user_email ON (stw.user_id.id = stw.user_email.id AND stw.user_id.user_id = stw.user_email.user_id) WHERE stw.user_email.email = $1;"
	  (slot-value function 'stw.db::sql-query)))))

(define-test sql-express...
  :parent stw-db
  
  (let ((*user* (make-instance 'user :email "liam@foobar.com")))
    (with-active-layers (insert-node)
      (let ((procedure (generate-procedure *user* nil)))
	(is equal
	    '("INSERT INTO stw.user_base DEFAULT VALUES RETURNING user_base.id INTO _user_base_id;"
	      "INSERT INTO stw.user_id (id) VALUES (_user_base_id) RETURNING user_id.user_id INTO _user_id_user_id;"
	      "INSERT INTO stw.user_email (user_id, id, email) SELECT _user_id_user_id, _user_base_id, email FROM UNNEST ($3);"
	      "_user_id := _user_id_user_id;" "_id := _user_base_id;")
	    (slot-value procedure 'stw.db::sql-list))))

    (with-active-layers (update-node)
      (setf (slot-value *user* 'id) 1)
      (let ((clone (clone-object *user*)))
	(setf (slot-value *user* 'email) "liam@foobaz.com")
	(let ((procedure (generate-procedure *user* clone)))
	  (is equal
	      '("UPDATE stw.user_email SET email = $1 WHERE email = $2 AND id = $3;" "RETURN;")
	      (slot-value procedure 'stw.db::sql-list)))))

    (with-active-layers (delete-node)
      (let ((procedure (generate-procedure *user* nil)))
	(is equal
	    '("DELETE FROM stw.user_base WHERE id = $1;")
	    (slot-value procedure 'stw.db::sql-list))))))

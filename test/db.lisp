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


(define-key-table user-base () id)

(define-db-table user-account ()
  ((id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (password :col-type :text)
   (created-on :col-type :timestamptz
	       :lock-value t
	       :default (now))
   (created-by :col-type :integer
	       :not-null t
	       :foreign-key (:table user-base
			     :column id
			     :on-delete :cascade
			     :on-update :cascade
			     :no-join t))
   (validated :col-type :boolean
	      :not-null t
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


(define-interface-node user (user-base user-id)
  ((emails :maps-table user-email :maps-column email :type list)))


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
	 :col-type :text)
   (ip :not-null t
       :col-type :text)))


(define-db-table user-handle ()
  ((id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (handle :col-type :text
	   :not-null t)))


(define-db-table user-url ()
  ((id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (url :col-type :text
	:not-null t)))


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
  ((sites :maps-table user-site :maps-columns (site ip) :express-as-type :alist :type list)))


(define-db-table current-user ()
  ((id :col-type :integer
       :primary-key t
       :root-key t
       :foreign-key (:table user-base
		     :column id
		     :on-delete :cascade
		     :on-update :cascade))
   (current-timestamp :col-type :timestamptz
		      :lock-value t
		      :default (now))))


(define-interface-node active-user (current-user)
  ())
  


;;;;; tests

(define-test tables...
  :parent stw-db
  (is equal
      `(user-base user-site user-url user-handle user-name user-account user-id user-email)
      (stw.db::tables (find-class 'account))))


(define-test check-schema
  :parent stw-db
  (with-active-layers (db-layer)
    (is string=
	"CREATE SCHEMA IF NOT EXISTS stw_test_schema"
	(create-schema *schema*))
    (is string=
	"SET search_path TO stw_test_schema, public"
	(set-schema *schema*))
    (is string=
	"GRANT ALL PRIVILEGES ON SCHEMA stw_test_schema TO liam"
	(set-privileged-user *schema* "liam"))))


(define-test creating-table...etc
  :parent stw-db
  (with-active-layers (db-table-layer)
    (is string=
	"CREATE TABLE IF NOT EXISTS stw_test_schema.user_account (id INTEGER NOT NULL, password TEXT, created_on TIMESTAMPTZ DEFAULT NOW(), created_by INTEGER NOT NULL, validated BOOLEAN NOT NULL DEFAULT 'f', PRIMARY KEY (id));"
	(create-table-statement (find-class 'user-account)))

    (is equal
	'("IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stw_test_schema_user_account_created_by_fkey') THEN ALTER TABLE stw_test_schema.user_account ADD CONSTRAINT stw_test_schema_user_account_created_by_fkey FOREIGN KEY (created_by) REFERENCES stw_test_schema.user_base (id) ON UPDATE CASCADE ON DELETE CASCADE; end if;"
	  "IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stw_test_schema_user_account_id_fkey') THEN ALTER TABLE stw_test_schema.user_account ADD CONSTRAINT stw_test_schema_user_account_id_fkey FOREIGN KEY (id) REFERENCES stw_test_schema.user_base (id) ON UPDATE CASCADE ON DELETE CASCADE; end if;")
	(foreign-keys-statements (find-class 'user-account)))

    ;; don't know why but this always fails. The expected result is correct
    (is equal 
	'("CREATE INDEX IF NOT EXISTS stw_test_schema_user_email_user_id_idx ON stw_test_schema.user_email (user_id);"
	  "CREATE INDEX IF NOT EXISTS stw_test_schema_user_email_id_idx ON stw_test_schema.user_email (id);")
	(index-statement (find-class 'user-email)))

    ;; only produces a statement for classes with tables that have a non-indexed foreign key.
    ;; Primary keys are indexed by default.
    (false (index-statement (find-class 'validate)))))


(defvar *user*) 
(defvar *account*)
(defvar *active-user*)

(define-test table-order...
  :parent stw-db
  (with-active-layers (insert-node)

    (let ((*account* (make-instance 'account :name "liam" :emails '("foo@bar.com")
					     :password "asdfasdf" :url "foobar.com"
					     :created-by 1 :validated t)))
      (is equal
	`(user-base user-url user-name user-account user-id user-email)
	  (with-active-layers (insert-node)
	    (stw.db::include-tables *account*)))
      (true (stw.db::slot-to-go *account* (find-slot-definition (find-class 'user-email) 'email 'db-column-slot-definition)))
      (false (stw.db::slot-to-go *account* (find-slot-definition (find-class 'user-handle) 'handle 'db-column-slot-definition)))

      (setf (slot-value *account* 'handle) "foo"
	    (slot-value *account* 'emails) nil)

      (is equal
	  `(user-base user-url user-handle user-name user-account user-id)
	  (with-active-layers (insert-node)
	    (stw.db::include-tables *account*)))
      (false (stw.db::slot-to-go *account* (find-slot-definition (find-class 'user-email) 'email 'db-column-slot-definition)))
      (true (stw.db::slot-to-go *account* (find-slot-definition (find-class 'user-handle) 'handle 'db-column-slot-definition))))))


(define-test keyword...
  :parent stw-db
  (let ((*active-user* (make-instance 'active-user :id 1)))
    (is string= (slot-value (find-class 'current-user) 'stw.db::table) "\"current_user\"")
    (is string= (slot-value (find-slot-definition (find-class 'current-user) 'current-timestamp 'db-column-slot-definition)
			    'stw.db::column-name)
	"\"current_timestamp\"")
    (is string=
	"CREATE TABLE IF NOT EXISTS stw_test_schema.\"current_user\" (id INTEGER NOT NULL, \"current_timestamp\" TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (id));"
	(with-active-layers (db-table-layer)
	  (create-table-statement (find-class 'current-user))))))
  

(define-test mapping...
  :parent stw-db
  (let ((*account* (make-instance 'account :name "liam" :emails "foo@bar.com"
					   :password "asdfasdf" :url "foobar.com"
					   :created-by 1 :validated t)))
    (with-active-layers (db-layer)
      (of-type stw.db::slot-mapping (stw.db::match-mapping-node (class-of *account*) (find-class 'user-site)))
      (let ((slot (find-slot-definition (find-class 'user-site) 'site 'db-column-slot-definition)))
	(of-type 'stw.db::slot-mapping (stw.db::match-mapping-node (class-of *account*) slot))))))


(define-test dispatching...
  :parent stw-db 
  :depends-on (table-order...)

  (let ((*account* (make-instance 'account :name "liam" :emails '("foo@bar.com")
					   :password "asdfasdf" :url "foobar.com"
					   :created-by 1 :validated t))
	(*user* (make-instance 'user :emails '("liam@foobar.com"))))

    (setf (slot-value *account* 'handle) "foo"
	  (slot-value *account* 'emails) nil)

    (with-active-layers (db-interface-layer)

      (let ((format-components (stw.db::sql-typed-array (find-class 'user-account))))
	(is string= "ARRAY[ ROW (~a, ~a, ~a)]::stw_test_schema.user_account_type[]" (car format-components))
	(is eql 3 (length (cadr format-components)))
	(loop
	  for slot in (cadr format-components)
	  do (of-type 'db-column-slot-definition slot)))

      (let ((foo-slot-definition (find-slot-definition (class-of *account*) 'handle 'db-column-slot-definition)))
	(is string= "E'foo'" (stw.db::prepare-value% foo-slot-definition (slot-value *account* 'handle))))

      (with-active-layers (insert-node)
	(let ((procedure (generate-procedure *user* nil)))
	  (of-type stw.db::procedure procedure)
	  (is equal
	      '((:OUT "_id" :INTEGER) (:OUT "_user_id" :INTEGER)
		(:IN "stw_test_schema.user_email_type[]"))
	      (slot-value procedure 'stw.db::args))
	  (is string= "CALL stw_test_schema.user_insert_1e8ac368_5f7a_37e0_8bc1_9b5550350b69 (null, null, ARRAY[ ~{ROW (~a)~^, ~}]::stw_test_schema.user_email_type[])"
	      (slot-value procedure 'stw.db::p-control))
	  (is string= "CALL stw_test_schema.user_insert_1e8ac368_5f7a_37e0_8bc1_9b5550350b69 (null, null, ARRAY[ ROW (E'liam@foobar.com')]::stw_test_schema.user_email_type[])"
	      (dispatch-statement *user* procedure))))

      (setf (slot-value *account* 'emails) '("foo@bar.com"))

      (let ((format-components (stw.db::sql-typed-array (find-class 'user-email))))
	(is string= "ARRAY[ ROW (~a)]::stw_test_schema.user_email_type[]" (car format-components))
	(is eql 1 (length (cadr format-components)))
	(of-type 'db-column-slot-definition (caadr format-components)))

      (with-active-layers (update-node)
	(let ((clone (clone-object *account*)))
	  (setf (slot-value clone 'emails) '("bar@foo.com"))
	  (fail (stw.db::match-root-keys *account* clone) 'null-key-error "Null value for key ID in class ACCOUNT.")
	  (setf (slot-value *account* 'id) 1
		(slot-value clone 'id) 2)
	  (fail (stw.db::match-root-keys *account* clone) 'update-key-value-error "Expected value: 1. Received value: 2.")
	  (setf (slot-value clone 'id) 1)
	  (let ((procedure (generate-procedure *account* clone)))
	    (of-type stw.db::procedure procedure)
	    (is equal
		'(("stw_test_schema.user_email_id") (:inout "delete_emails" "stw_test_schema.user_email_type[]"))
		(slot-value procedure 'stw.db::args))
	    (is string=
		"CALL stw_test_schema.account_update_1e8ac368_5f7a_37e0_8bc1_9b5550350b69 (~a, ARRAY[ ~{ROW (~a)~^, ~}]::stw_test_schema.user_email_type[])"
		(slot-value procedure 'stw.db::p-control))
	    (is string=
		"CALL stw_test_schema.account_update_1e8ac368_5f7a_37e0_8bc1_9b5550350b69 (1, ARRAY[ ROW (E'foo@bar.com')]::stw_test_schema.user_email_type[])"
		(update-op-dispatch-statement *account* clone procedure)))))
      
      (let* ((map (stw.db::match-mapping-node (find-class 'account) (find-class 'user-site)))
	     (format-components (stw.db::sql-typed-array map)))
	(of-type stw.db::slot-mapping map)
	(is string= "ARRAY[ ~{ROW (~{~a, ~a~})~^, ~}]::stw_test_schema.user_site_type[]" (car format-components))

	(with-active-layers (insert-table)
	  (setf (slot-value *account* 'sites) `(((site . "foo.com") (ip . "123.345.234.1"))
						((site . "bar.com") (ip . "234.987.1.1"))
						((site . "baz.com") (ip . "234.234.234.2"))))
	  (let ((procedure (generate-procedure *account* (find-class 'user-site))))
	    (of-type stw.db::procedure procedure)
	    (is string=
		"CALL stw_test_schema.user_site_insert (~a, ARRAY[ ~{ROW (~{~a, ~a~})~^, ~}]::stw_test_schema.user_site_type[])"
		(slot-value procedure 'stw.db::p-control))
	    (is equal
		'(("stw_test_schema.user_site_id") (:inout "insert_sites" "stw_test_schema.user_site_type[]"))
		(slot-value procedure 'stw.db::args))
	    (is string=
		"CALL stw_test_schema.user_site_insert (1, ARRAY[ ROW (E'foo.com', E'123.345.234.1'), ROW (E'bar.com', E'234.987.1.1'), ROW (E'baz.com', E'234.234.234.2')]::stw_test_schema.user_site_type[])"
		(dispatch-statement *account* procedure))))
	
	(with-active-layers (delete-table)
	  (let ((procedure (generate-procedure *account* (find-class 'user-site))))
	    (of-type stw.db::procedure procedure)
	    (is string=
		"CALL stw_test_schema.user_site_delete (~a, ARRAY[ ~{ROW (~{~a, ~a~})~^, ~}]::stw_test_schema.user_site_type[])"
		(slot-value procedure 'stw.db::p-control))
	    (is equal
		'(("stw_test_schema.user_site_id") (:INOUT "delete_sites" "stw_test_schema.user_site_type[]"))
		(slot-value procedure 'stw.db::args))
	    (is string=
		"CALL stw_test_schema.user_site_delete (1, ARRAY[ ROW (E'foo.com', E'123.345.234.1'), ROW (E'bar.com', E'234.987.1.1'), ROW (E'baz.com', E'234.234.234.2')]::stw_test_schema.user_site_type[])"
		(dispatch-statement *account* procedure))))))))



(define-test retrieving...
  :parent stw-db
  (with-active-layers (retrieve-node)
    (let* ((*user* (make-instance 'user :emails '("liam@foobar.com")))
	   (function (generate-procedure *user* nil)))
      (of-type 'stw.db::db-function function)
      (is equal
	  '((:IN "_emails" "TEXT[]"))
	  (slot-value function 'stw.db::args))
      (is string=
	  "user_retrieve_df014172_2ec1_39a0_b186_c8d9bf345627"
	  (slot-value function 'stw.db::name))
      (is string=
	  "SELECT * FROM stw_test_schema.user_retrieve_df014172_2ec1_39a0_b186_c8d9bf345627 (ARRAY[~{~a~^, ~}])"
	  (slot-value function 'stw.db::p-control))
      (is string=
	  "SELECT stw_test_schema.user_base.id, stw_test_schema.user_id.user_id, emails.emails FROM stw_test_schema.user_base INNER JOIN stw_test_schema.user_id ON (stw_test_schema.user_base.id = stw_test_schema.user_id.id) INNER JOIN ((SELECT stw_test_schema.user_email.id, stw_test_schema.user_email.user_id FROM stw_test_schema.user_email WHERE stw_test_schema.user_email.email IN (SELECT UNNEST ($1)) GROUP BY stw_test_schema.user_email.id, stw_test_schema.user_email.user_id)) emails_key ON (stw_test_schema.user_id.id = emails_key.id AND stw_test_schema.user_id.user_id = emails_key.user_id) INNER JOIN ((SELECT ARRAY_AGG((stw_test_schema.user_email.email)) AS emails, stw_test_schema.user_email.user_id, stw_test_schema.user_email.id FROM stw_test_schema.user_email GROUP BY stw_test_schema.user_email.user_id, stw_test_schema.user_email.id)) emails ON (emails_key.id = emails.id AND emails_key.user_id = emails.user_id);"
	  (slot-value function 'stw.db::sql-query)))))

(define-test sql-express...
  :parent stw-db
  
  (let ((*user* (make-instance 'user :emails '("liam@foobar.com"))))
    (with-active-layers (insert-node)
      (let ((procedure (generate-procedure *user* nil)))
	(is equal
	    '("INSERT INTO stw_test_schema.user_base DEFAULT VALUES RETURNING user_base.id INTO _user_base_id;"
	      "INSERT INTO stw_test_schema.user_id (id) VALUES (_user_base_id) RETURNING user_id.user_id INTO _user_id_user_id;"
	      "INSERT INTO stw_test_schema.user_email (user_id, id, email) SELECT _user_id_user_id, _user_base_id, email FROM UNNEST ($3);" "_user_id := _user_id_user_id;" "_id := _user_base_id;")
	    (slot-value procedure 'stw.db::sql-list))))

    (with-active-layers (update-node)
      (setf (slot-value *user* 'id) 1)
      (let ((clone (clone-object *user*)))
	(setf (slot-value *user* 'emails) '("liam@foobaz.com"))
	(let ((procedure (generate-procedure *user* clone)))
	  (is equal
	      '("DELETE FROM stw_test_schema.user_email WHERE id = $1 AND email IN (SELECT email FROM UNNEST ($2));" "RETURN;")
	      (slot-value procedure 'stw.db::sql-list)))))

    (with-active-layers (delete-node)
      (let ((procedure (generate-procedure *user* nil)))
	(is equal
	    '("DELETE FROM stw_test_schema.user_base WHERE id = $1;")
	    (slot-value procedure 'stw.db::sql-list))))))

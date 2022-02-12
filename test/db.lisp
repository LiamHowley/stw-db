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
       :primary-key t
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
  ()
  (:key-columns . ((:table user-base :column id))))



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
  ((sites :maps-table user-site
	  :maps-column site))
  (:key-columns . ((:table user-base :column id))))



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
	"CREATE TABLE IF NOT EXISTS stw.user_account (id INTEGER NOT NULL, password TEXT NOT NULL, created_on TIMESTAMPTZ DEFAULT NOW(), created_by INTEGER NOT NULL, validated BOOLEAN DEFAULT 'f', PRIMARY KEY (id))"
	(create-statement (find-class 'user-account)))

    (is equal
	'("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stw_user_account_created_by_fkey') THEN ALTER TABLE stw.user_account ADD CONSTRAINT stw_user_account_created_by_fkey FOREIGN KEY (created_by) REFERENCES stw.user_base (id) ON UPDATE CASCADE ON DELETE CASCADE; end if; END; $$;"
	  "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'stw_user_account_id_fkey') THEN ALTER TABLE stw.user_account ADD CONSTRAINT stw_user_account_id_fkey FOREIGN KEY (id) REFERENCES stw.user_base (id) ON UPDATE CASCADE ON DELETE CASCADE; end if; END; $$;")
	(foreign-keys-statements (find-class 'user-account)))

    ;; don't know why but this always fails. The expected result is correct
    (is equal 
	'("CREATE INDEX IF NOT EXISTS stw_user_email_user_id_idx ON stw.user_email (user_id)"
	  "CREATE INDEX IF NOT EXISTS stw_user_email_id_idx ON stw.user_email (id)")
	(index-statement (find-class 'user-email)))

    ;; only produces a statement for classes with tables that have a non-indexed foreign key.
    ;; Primary keys are indexed by default.
    (false (index-statement (find-class 'validate)))))



(defvar *account*)

(define-test inserting...
  :parent stw-db 
  (let ((*account* (make-instance 'account :name "liam" :email "foo@bar.com"
					   :password "asdfasdf" :url "jaggedc.com"
					   :created-by 1 :validated t)))
    (with-active-layers (db-interface-layer)

      (is equal
	  `(user-base user-id user-email user-url user-name user-account)
	  (stw.db::include-tables *account*))
      (true (stw.db::slot-to-go *account* (stw.meta:find-slot-definition (find-class 'user-email) 'email)))
      (false (stw.db::slot-to-go *account* (stw.meta:find-slot-definition (find-class 'user-handle) 'handle)))

      (setf (slot-value *account* 'handle) "foo"
	    (slot-value *account* 'email) nil)

      (is equal
	  `(user-base user-id user-url user-handle user-name user-account)
	  (stw.db::include-tables *account*))
      (false (stw.db::slot-to-go *account* (stw.meta:find-slot-definition (find-class 'user-email) 'email)))
      (true (stw.db::slot-to-go *account* (stw.meta:find-slot-definition (find-class 'user-handle) 'handle)))

      (let ((format-components (stw.db::sql-typed-array (find-class 'user-account))))
	(is string= "ARRAY[(~a, ~a, ~a)]::stw.user_account_type[]" (car format-components))
	(is eql 3 (length (cadr format-components)))
	(loop
	  for slot in (cadr format-components)
	  do (of-type 'db-column-slot-definition slot))
	(is string= "ARRAY[('t', 1, 'asdfasdf')]::stw.user_account_type[]"
	    (stw.db::process-values *account* format-components (slot-value (find-class 'user-account) 'stw.db::mapped-by))))

      (setf (slot-value *account* 'email) "foo@bar.com")

      (let ((format-components (stw.db::sql-typed-array (find-class 'user-email))))
	(is string= "ARRAY[(~a)]::stw.user_email_type[]" (car format-components))
	(is eql 1 (length (cadr format-components)))
	(of-type 'db-column-slot-definition (caadr format-components))
	(is string= "ARRAY[('(foo@bar.com)')]::stw.user_email_type[]"
	    (stw.db::process-values *account* format-components (slot-value (find-class 'user-email) 'stw.db::mapped-by))))
	  
      (let ((format-components (stw.db::sql-typed-array (find-class 'user-site))))
	(is string= "ARRAY[~{(~a)~^, ~}]::stw.user_site_type[]" (car format-components))

	(setf (slot-value *account* 'sites) '("foo.com" "bar.com" "baz.com"))
	(is string= "ARRAY[('(foo.com)'), ('(bar.com)'), ('(baz.com)')]::stw.user_site_type[]"
	    (stw.db::process-values *account* format-components (slot-value (find-class 'user-site) 'stw.db::mapped-by)))))))

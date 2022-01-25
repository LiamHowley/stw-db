(in-package stw.db.test)

;;;; setting up

(setf *schema* "stw")

(define-db-table user-base ()
  ((id :col-type :serial
       :primary-key t
       :return-on (:insert))))


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
	      :not-null t)))


(define-db-table user-id ()
  ((user-id :col-type :serial
	    :primary-key t
	    :return-on (:insert))
   (id :col-type :integer
       :not-null t
       :foreign-key (:table user-base
			    :column id
			    :on-delete :cascade
			    :on-update :cascade))))


(define-db-table user-email ()
  ((email :col-type :text
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
	 :col-type :text)))


(define-db-table user-handle ()
  ((handle :col-type :text
	   :primary-key t)
   (id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
			    :column id
			    :on-delete :cascade
			    :on-update :cascade))))


(define-db-table user-url ()
  ((url :col-type :text :primary-key t)
   (id :col-type :integer
       :primary-key t
       :foreign-key (:table user-base
			    :column id
			    :on-delete :cascade
			    :on-update :cascade))))


(define-db-table validate ()
  ((vkey :col-type :serial
	 :primary-key t
	 :return-on :insert)
   (token :col-type :text
	  :not-null t)
   (expires_on :col-type :timestamp
	       :not-null t)))


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
    (user-base user-id user-email user-account user-name user-handle user-url user-validate)
  ((sites :maps-table user-site))
  :key-column (:table user-base :column id)
  :tables (user-base
	   user-id
	   user-email
	   user-site
	   user-account
	   user-handle
	   user-url
	   user-validate
	   user-name))



;;;;; tests

(with-active-layers (db-layer)

  (define-test check-schema
    :parent stw-db
    (is string=
	"CREATE SCHEMA IF NOT EXISTS stw"
	(create-schema))
    (is string=
	"SET search_path TO stw, public"
	(set-schema))
    (is string=
	"GRANT ALL PRIVILEGES ON SCHEMA stw TO liam"
	(set-privileged-user "liam"))))


(with-active-layers (db-table-layer)

  (is (create-statement (find-class 'user-account))
      "CREATE TABLE IF NOT EXISTS stw.user_account (id INTEGER NOT NULL, password TEXT NOT NULL, created_on TIMESTAMPTZ DEFAULT NOW(), created_by INTEGER NOT NULL, validated BOOLEAN NOT NULL, PRIMARY KEY (id))")

  (is equal
      '("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = stw_user_base_id_fkey) THEN ALTER TABLE stw.user_account ADD CONSTRAINT stw_user_base_id_fkey FOREIGN KEY (id) REFERENCES user_base (id) ON UPDATE CASCADE ON DELETE CASCADE; end if; END; $$;"
	"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = stw_user_base_created_by_fkey) THEN ALTER TABLE stw.user_account ADD CONSTRAINT stw_user_base_created_by_fkey FOREIGN KEY (created_by) REFERENCES user_base (id) ON UPDATE CASCADE ON DELETE CASCADE; end if; END; $$;")
      (foreign-keys-statements (find-class 'user-account)))

  ;; don't know why but this always fails. The expected result is correct
  (is equal 
      '("CREATE INDEX IF NOT EXISTS stw_user_email_user_id_idx ON stw.user_email (user_id)"
	"CREATE INDEX IF NOT EXISTS stw_user_email_id_idx ON stw.user_email (id)")
      (index-statement (find-class 'user-email)))

  ;; only produces a statement for classes with tables that have a non-indexed foreign key.
  ;; Primary keys are indexed by default.
  (false (index-statement (find-class 'validate))))
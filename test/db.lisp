(in-package stw.db.test)

;;;; setting up

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



;;;;; tests



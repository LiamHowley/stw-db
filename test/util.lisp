(in-package stw.db.test)


(define-test syntax-functions
  :parent stw-db
  (is string= (db-syntax-prep 'user-email) "user_email")
  (is string= (db-syntax-prep "USER-EMAIL") "user_email")
  (is string= (set-sql-name 'user-email 'email) "user_email.email")
  (is string= (set-sql-name "user-email" "email") "user_email.email"))


(define-test prefix-to-infix
  :parent stw-db
  (is equal
      (infill-column '(or (and (> 3) (< 5)) (and (> 10) (< 13))) 'id)
      '(OR (AND (> ID 3) (< ID 5)) (AND (> ID 10) (< ID 13))))
  (is equal
      (infill-column '(or (and (> 3 different-column) (< 5 and-another)) (and (> 10) (< 13))) 'id)
      '(OR (AND (> 3 DIFFERENT-COLUMN) (< 5 AND-ANOTHER)) (AND (> ID 10) (< ID 13)))))

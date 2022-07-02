(in-package stw.db)

(defvar *reserved-keywords*
  '("all" "analyse" "analyze" "and" "any" "array" "as" "asc" "asymmetric"
 "authorization" "binary" "both" "case" "cast" "check" "collate" "collation"
 "column" "concurrently" "constraint" "create" "cross" "current_catalog"
 "current_date" "current_role" "current_schema" "current_time"
 "current_timestamp" "current_user" "default" "deferrable" "desc" "distinct"
 "do" "else" "end" "except" "false" "fetch" "for" "foreign" "freeze" "from"
 "full" "grant" "group" "having" "ilike" "in" "initially" "inner" "intersect"
 "into" "is" "isnull" "join" "lateral" "leading" "left" "like" "limit"
 "localtime" "localtimestamp" "natural" "not" "notnull" "null" "offset" "on"
 "only" "or" "order" "outer" "overlaps" "placing" "primary" "references"
 "returning" "right" "select" "session_user" "similar" "some" "symmetric"
 "table" "tablesample" "then" "to" "trailing" "true" "union" "unique" "user"
 "using" "variadic" "verbose" "when" "where" "window" "with"))


(defvar *reserved-function/type-names*
  '("all" "analyse" "analyze" "and" "any" "array" "as" "asc" "asymmetric"
 "between" "bigint" "bit" "boolean" "both" "case" "cast" "char" "character"
 "check" "coalesce" "collate" "column" "constraint" "create" "current_catalog"
 "current_date" "current_role" "current_time" "current_timestamp"
 "current_user" "dec" "decimal" "default" "deferrable" "desc" "distinct" "do"
 "else" "end" "except" "exists" "extract" "false" "fetch" "float" "for"
 "foreign" "from" "grant" "greatest" "group" "grouping" "having" "in"
 "initially" "inout" "int" "integer" "intersect" "interval" "into" "lateral"
 "leading" "least" "limit" "localtime" "localtimestamp" "national" "nchar"
 "none" "normalize" "not" "null" "nullif" "numeric" "offset" "on" "only" "or"
 "order" "out" "overlay" "placing" "position" "precision" "primary" "real"
 "references" "returning" "row" "select" "session_user" "setof" "smallint"
 "some" "substring" "symmetric" "table" "then" "time" "timestamp" "to"
 "trailing" "treat" "trim" "true" "union" "unique" "user" "using" "values"
 "varchar" "variadic" "when" "where" "window" "with" "xmlattributes"
 "xmlconcat" "xmlelement" "xmlexists" "xmlforest" "xmlnamespaces" "xmlparse"
 "xmlpi" "xmlroot" "xmlserialize" "xmltable"))


(defun make-reservations (list)
  (let ((hash (make-hash-table
	       :test #'equal
	       :size (length list))))
    (loop
      for word in list
      do (setf (gethash word hash) (concatenate 'string "\"" word "\"")))
    #'(lambda (word)
	(or (gethash word hash)
	    word))))

(defvar *reserved-keywords-filter*
  (make-reservations *reserved-keywords*))

(defvar *reserved-function/type-filter*
  (make-reservations *reserved-function/type-names*))

;;(defun make-reserved-keyword-predicate ()
;;  (with-reservations *reserved-keywords-p*
;;    "catdesc = 'reserved' OR catdesc = 'reserved (can be function or type name)';"))
;;
;;(defun make-reserved-function/type-names-predicate ()
;;  (with-reservations *reserved-function/type-p*
;;    "catdesc = 'reserved' OR catdesc = 'unreserved (cannot be function or type name)';"))
;;
;;(defun reserved-keywords ()
;;  (or (fboundp '*reserved-keywords-p*)
;;      (make-reserved-keyword-predicate)))
;;
;;(defun reserved-function/type-names ()
;;  (or (fboundp '*reserved-keywords-p*)
;;      (make-reserved-function/type-names-predicate)))

#+ ()(exec-query *db* (format nil "SELECT word FROM pg_get_keywords() WHERE ~a;" ,where)
			     (row-reader (fields)
			       (let (list)
				 (loop 
				   while (next-row)
				   do (loop 
					for field across fields
					do (push (next-field field) list)))
				 (nreverse list))))

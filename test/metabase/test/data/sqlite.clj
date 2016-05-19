(ns metabase.test.data.sqlite
  (:require [clojure.string :as s]
            [honeysql.core :as hsql]
            metabase.driver.sqlite
            (metabase.test.data [generic-sql :as generic]
                                [interface :as i])
            [metabase.util :as u]
            [metabase.util.honeysql-extensions :as hx])
  (:import metabase.driver.sqlite.SQLiteDriver))

(defn- database->connection-details [context {:keys [short-lived?], :as dbdef}]
  {:short-lived? short-lived?
   :db           (str (i/escaped-name dbdef) ".sqlite")})

(def ^:private ^:const field-base-type->sql-type
  {:BigIntegerField "BIGINT"
   :BooleanField    "BOOLEAN"
   :CharField       "VARCHAR(254)"
   :DateField       "DATE"
   :DateTimeField   "DATETIME"
   :DecimalField    "DECIMAL"
   :FloatField      "DOUBLE"
   :IntegerField    "INTEGER"
   :TextField       "TEXT"
   :TimeField       "TIME"})

(defn- load-data-stringify-dates
  "Our SQLite JDBC driver doesn't seem to like Dates/Timestamps or Booleans so just convert them to strings or numbers, respectively, before INSERTing them into the Database."
  [insert!]
  (fn [rows]
    (insert! (for [row rows]
               (into {} (for [[k v] row]
                          [k (cond
                               (instance? java.util.Date v) (hsql/call :datetime (hx/literal (u/date->iso-8601 v)))
                               (true? v)                    1
                               (false? v)                   0
                               :else                        v)]))))))

(u/strict-extend SQLiteDriver
  generic/IGenericSQLDatasetLoader
  (merge generic/DefaultsMixin
         {:add-fk-sql                (constantly nil) ; TODO - fix me
          :create-db-sql             (constantly nil)
          :drop-db-if-exists-sql     (constantly nil)
          :execute-sql!              generic/sequentially-execute-sql!
          :load-data!                (generic/make-load-data-fn load-data-stringify-dates generic/load-data-chunked)
          :pk-sql-type               (constantly "INTEGER")
          :field-base-type->sql-type (u/drop-first-arg field-base-type->sql-type)})
  i/IDatasetLoader
  (merge generic/IDatasetLoaderMixin
         {:database->connection-details (u/drop-first-arg database->connection-details)
          :engine                       (constantly :sqlite)}))

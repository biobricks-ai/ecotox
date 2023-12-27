(ns biobricks.ecotox
  (:require [babashka.fs :as fs]
            [clojure-csv.core :as csv]
            [clojure.java.io :as io]
            [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))

;; Known bad lines and their legal replacements
(def replacement-lines
  {"2597337|2263201|None|NR|None|NR|None|NR|NR|None|None|24|None|NR|None|NR|h|None|LOEC|None|INC|BCM|None|GBCM/|\"UNCHARACTERISED PROTEIN (TR|M7BW30|M7BW30_CHEMY)\"|OV|None|None|NR|None|NR|None|NR|None|A|NR|None|100|None|NR|None|NR|ug/L|ONLY CONC TESTED|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|--|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|ASIG|P|<|0.01|None|NR|None|NR|None|U|None|R|None|NR|None|NR|None|NR|NR|None|None|NR|None|NR|None|NR|NR|None|None|NR|None|NR|None|NR|None|NR|None|NR|None|NR|None|NR|None|NR|CONC1/ONLY CONC TESTED// |None|03/25/2021|03/25/2021|None"
   "2597337|2263201|None|NR|None|NR|None|NR|NR|None|None|24|None|NR|None|NR|h|None|LOEC|None|INC|BCM|None|GBCM/|\"UNCHARACTERISED PROTEIN (TR/M7BW30/M7BW30_CHEMY)\"|OV|None|None|NR|None|NR|None|NR|None|A|NR|None|100|None|NR|None|NR|ug/L|ONLY CONC TESTED|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|--|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|None|ASIG|P|<|0.01|None|NR|None|NR|None|U|None|R|None|NR|None|NR|None|NR|NR|None|None|NR|None|NR|None|NR|NR|None|None|NR|None|NR|None|NR|None|NR|None|NR|None|NR|None|NR|None|NR|CONC1/ONLY CONC TESTED// |None|03/25/2021|03/25/2021|None"})

(defn db-files [dir]
  (->> (fs/match dir "regex:.*\\.txt" {:recursive true})
       (remove #(str/starts-with? (fs/file-name %) "release_notes_"))))

(defn ascii-rows [file]
  (->> file fs/file io/reader line-seq
       (map #(replacement-lines % %))
       (map #(str/split % #"\|" -1))))

(defn write-csv [file rows]
  (with-open [writer (io/writer (fs/file file))]
    (doseq [row (map #(@#'csv/quote-and-escape-row % "," \" true) rows)]
      (.write writer row)
      (.write writer "\n"))))

(defn write-parquet [file rows]
  (fs/with-temp-dir [dir {:prefix "biobricks-ecotox"}]
    (let [csv (fs/path dir "input.csv")
          parquet (fs/path dir "output.parquet")]
      (write-csv csv rows)
      (let [{:keys [exit] :as process} (sh/sh "csv2parquet" (str csv) (str parquet))]
        (when-not (zero? exit)
          (throw (ex-info (str "csv2parquet exited with code " exit)
                          {:process process}))))
      (fs/move parquet file {:replace-existing true})))
  file)

(defn build-parquet [{:keys [db output]}]
  (fs/create-dirs output)
  (doseq [file (db-files db)]
    (let [rows (ascii-rows file)
          filename (-> (fs/file-name file)
                       (str/replace #"\.txt$" ""))
          parquet-file (fs/path output (str filename ".parquet"))]
      (log/info "Writing" (fs/file-name parquet-file))
      (write-parquet parquet-file rows))))

(comment
  (let [out (fs/path "brick")]
    (fs/delete-tree out)
    (build-parquet {:db "data/ecotox_ascii" :output (str out)}))
  )

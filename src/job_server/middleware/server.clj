(ns job-server.middleware.server
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.string :as s]
            [ring.util.response :as resp]
            [taoensso.timbre :as timbre :refer [trace tracef debug warn error]])
  (:import [java.time LocalDateTime]))

(def ^:private max-requests 10)

(def ^:private max-jobs 1)

;; #region job-mapping spec

(spec/def ::var-fn (spec/and var? #(fn? (var-get %))))
(spec/def ::fn-or-var-symbol (spec/or :fn fn?
                                      :var ::var-fn))

(spec/def ::extract-args-fn (spec/fspec :args (spec/cat :params map?)))
(spec/def ::extract-args
  (spec/or :fn ::extract-args-fn
           :var (spec/and ::var-fn ::extract-args-fn)))

(spec/def ::executor ::fn-or-var-symbol)

;; Spec for the job config
(spec/def ::job-config
  (spec/keys :req-un [::extract-args ::executor]))

;; Predicate to check if a keyword is namespaced
(defn namespaced-keyword? [x]
  (and (keyword? x)
       (namespace x)))

;; Top-level spec for the entire map with namespaced key requirement
(spec/def ::job-mapping
  (spec/map-of (spec/and keyword? namespaced-keyword?) ::job-config))

;; #endregion

;#region spec utils

(defn- try-conform [spec x]
  (let [conformed (spec/conform spec x)]
    (if (= conformed :clojure.spec.alpha/invalid)
      (throw (ex-info (spec/explain-str spec x)
                      (spec/explain-data spec x)))
      conformed)))

;#endregion

;; region http utils

(defn- ok
  ([body]
   (-> (resp/response body)
       (resp/status 200)))
  ([]
   (ok nil)))

(defn- accepted
  ([content-location-uri location-uri body]
   (-> (resp/response body)
       (resp/status 202)
       (resp/header "Content-Location" content-location-uri)
       (resp/header "Location" location-uri)))
  ([content-location-uri location-uri]
   (accepted content-location-uri location-uri nil)))

(defn- not-found [uri-or-page-name]
  (-> (resp/not-found (str "Not found " uri-or-page-name))
      (resp/content-type "text")))

(defn- server-busy []
  (resp/status 503))

(defn- bad-request
  ([]
   (resp/bad-request {}))
  ([body]
   (resp/bad-request body)))

;; endregion

(def job-status-route-pattern-str "/job/([a-fA-F0-9\\-]+?)/status/?")

(defn- matches-job-status-route?
  ([uri route-prefix]
   (let [pattern (re-pattern (str route-prefix job-status-route-pattern-str))]
     (re-find pattern uri)))
  ([uri]
   (matches-job-status-route? uri "")))

(def job-resource-route-pattern-str "/job/([a-fA-F0-9\\-]+)/?")

(defn- matches-job-resource-route?
  ([uri route-prefix]
   (let [pattern (re-pattern (str route-prefix job-resource-route-pattern-str))]
     (re-find pattern uri)))
  ([uri]
   (matches-job-resource-route? uri "")))

(defn- handle-state-request [{:keys [::server-state] :as _request}]
  (ok (get @server-state :state)))

(defn- handle-history-request [{:keys [::server-state] :as _request}]
  (ok (get-in @server-state [:state :history])))

(defn- handle-jobs [{:keys [params ::server-state ::route-prefix] :as request}]
  (let [{job-domain "jobDomain"
         job-type "jobType"} params]
    (if-not (and job-domain job-type)
      (bad-request (merge {:job-domain job-domain
                           :job-type job-type}
                          {:message "jobDomain and jobType are required"}))
      (let [job-id (random-uuid)
            job {:id job-id
                 :type (keyword job-domain job-type)
                 :params params}
            input-chan (get-in @server-state [:server :input-chan])]
        (tracef "[handle-jobs] handling posted job %s <<<" job-id)
        (tracef "[handle-jobs] offering job %s <<<" job-id)
        (if (a/offer! input-chan job)
          (let [record-new-job (fn []
                                 (let [timestamp (LocalDateTime/now)]
                                   (tracef "[handle-jobs]   inserting pending job %s" job-id)
                                   (swap! server-state update-in [:state :pending] conj {:job-id job-id
                                                                                         :job-type job-type
                                                                                         :accept-time (str timestamp)})))
                create-response (fn []
                                  (let [{:keys [scheme server-name server-port]} request
                                        job-status-url (format "%s://%s:%s%s/job/%s/status" (name scheme) server-name server-port route-prefix job-id)
                                        job-resource-url (format "%s://%s:%s%s/job/%s" (name scheme) server-name server-port route-prefix job-id)]
                                    (println "route-prefix" route-prefix)
                                    (accepted job-status-url job-resource-url {"jobId" job-id})))]
            (record-new-job)
            (create-response))
          (do
            (warn "[handle-jobs] failed to offer job; jobDomain=%s; jobType=%s" job-domain job-type)
            (server-busy)))))))

(defn- handle-job-request [{:keys [uri ::server-state] :as _request}]
  (let [match (matches-job-resource-route? uri)
        job-id (get match 1)]

    (if-let [executor-output (->> (get-in @server-state [:state :history])
                                  (some (fn [job] (and (= job-id (str (:job-id job))) job)))
                                  (:executor-output))]
      (ok {:job-id job-id
           :job-output executor-output})
      (not-found {:job-id job-id}))))

(defn- handle-job-status-request [{:keys [uri ::server-state] :as _request}]
  (let [match (matches-job-status-route? uri)
        job-id (get match 1)
        {{:keys [pending history]} :state} @server-state
        job (cond
              (some (fn [{id :job-id}] (= (str id) job-id)) pending)
              (some (fn [{id :job-id :as job}] (and (= (str id) job-id) job)) pending)

              (some (fn [{id :job-id}] (= (str id) job-id)) history)
              (some (fn [{id :job-id :as job}] (and (= (str id) job-id) job)) history)

              :else
              nil)]
    (if job
      (ok job)
      (not-found {:job-id job-id}))))

(def ^:private routes {:api-state {:get #'handle-state-request}
                       :api-history {:get #'handle-history-request}
                       :api-job {:get #'handle-job-request}
                       :api-job-status {:get #'handle-job-status-request}
                       :api-jobs {:post #'handle-jobs}})

(defn- router [uri route-prefix]
  (cond
    (= uri (str route-prefix "/state"))
    :api-state

    (= uri (str route-prefix "/history"))
    :api-history

    (matches-job-status-route? uri route-prefix)
    :api-job-status

    (matches-job-resource-route? uri route-prefix)
    :api-job

    (= uri (str route-prefix "/jobs"))
    :api-jobs

    :else
    :not-found))

(defn- request->handler [request route-prefix]
  (let [uri (:uri request)
        method (:request-method request)
        handler (-> (router uri route-prefix)
                    (as-> route (get routes route))
                    (get method))]
    handler))

(defn- create-process [f queue-chan admin-chan]
  (a/thread
    (loop []
      (let [[work source-chan] (a/alts!! [admin-chan queue-chan] :priority true)]
        (when (and (= queue-chan source-chan)
                   (not (nil? work)))
          (f work)
          (recur))))))

(defn- create-input-process [input-chan admin-chan worker-chan]
  (let [f (fn [{job-id :id :as job}]
            (tracef "[input-process] handling %s" job-id)
            (tracef "[input-process]   blocking put to worker-chan")
            (a/>!! worker-chan job)
            (tracef "[input-process] handling %s complete" job-id))]
    (create-process f input-chan admin-chan)))

(defn- do-job [{:keys [id executor args] :as job}]
  (try
    (let [executor-output (executor args)]
      {:job-id id
       :job-status :job/success
       :executor-output (:job-server/output executor-output)})
    (catch Exception exception
      (error "END JOB (FAIL)" id exception)
      (debug job)
      {:job-id id
       :job-args args
       :job-status :job/failure
       :exception exception})))

(defn- create-worker-process [worker-chan admin-chan state-chan job-mapping]
  (let [f (fn [work]
            (let [{:keys [id type params]} work]
              (tracef "[worker-process] handling %s" id)
              ;; update job state to :started
              (trace "[worker-process]   blocking put (:started) to state-chan")
              (a/>!! state-chan {:operation :started
                                 :job-id id
                                 :timestamp (LocalDateTime/now)})
              (tracef "[worker-process]   resuming %s" id)
              (let [{:keys [extract-args executor]} (get job-mapping type)
                    args (try (extract-args params) (catch Exception _ {}))
                    job {:id id
                         :executor executor
                         :args args}

                    ;; run job
                    _ (tracef "[worker-proces]   executing job %s" id)
                    result (do-job job)
                    _ (tracef "[worker-process]   job %s complete" id)]
                ;; update job state to :completed
                (tracef "[worker-process]   blocking put (:complete) to state-chan")
                (a/>!! state-chan (assoc result
                                         :operation :completed
                                         :timestamp (LocalDateTime/now)))
                (tracef "[worker-process] handling %s complete" id))))]
    (create-process f worker-chan admin-chan)))

(defn- create-state-process [state-chan admin-chan server-state-ref]
  (let [f (fn [{:keys [job-id] :as result}]
            (let [pending-job?
                  (fn [job-id x] (= job-id (:job-id x)))

                  find-pending-job
                  (fn [job-id pending-jobs]
                    (some (fn [x]
                            (and (pending-job? job-id x)
                                 x))
                          pending-jobs))

                  completed-proc-error-update
                  (fn [{:keys [job-id job-args job-status timestamp] :as result} {:keys [pending history] :as state}]
                    (let [exception (:exception result)
                          exception-message (ex-message exception)
                          pending-job (find-pending-job job-id (:pending state))
                          {:keys [out err exit] :as _ex-data} (ex-data exception)
                          out (when out (slurp out))
                          err (when err (slurp err))]
                      (-> state
                          (assoc :pending (->> pending
                                               (remove (partial pending-job? job-id))
                                               (vec)))
                          (assoc :history (conj history (cond-> (merge pending-job {:job-id job-id
                                                                                    :job-status job-status
                                                                                    :end-time (str timestamp)})
                                                          (some? job-args) (assoc :job-args job-args)
                                                          (some? exception-message) (assoc :exception-message exception-message)
                                                          (some? exit) (assoc :exit exit)
                                                          (some? out) (assoc :out out)
                                                          (some? err) (assoc :err err)))))))

                  completed-error-update
                  (fn [{:keys [job-id job-args job-status timestamp] :as result} {:keys [pending history] :as state}]
                    (let [exception (:exception result)
                          exception-message (ex-message exception)
                          pending-job (find-pending-job job-id (:pending state))]
                      (-> state
                          (assoc :pending (->> pending
                                               (remove (partial pending-job? job-id))
                                               (vec)))
                          (assoc :history (conj history (cond-> (merge pending-job {:job-id job-id
                                                                                    :job-status job-status
                                                                                    :end-time (str timestamp)})
                                                          (some? job-args) (assoc :job-args job-args)
                                                          (some? exception-message) (assoc :exception-message exception-message)))))))

                  completed-success-update
                  (fn [{:keys [job-id job-args job-status timestamp executor-output]} {:keys [pending history] :as state}]
                    (let [pending-job (find-pending-job job-id (:pending state))]
                      (-> state
                          (assoc :pending (->> pending
                                               (remove (partial pending-job? job-id))
                                               (vec)))
                          (assoc :history (conj history (cond-> (merge pending-job {:job-id job-id
                                                                                    :job-status job-status
                                                                                    :end-time (str timestamp)})
                                                          job-args (assoc :job-args job-args)
                                                          executor-output (assoc :executor-output executor-output)))))))]

              (tracef "[state-process] handling %s" job-id)
              (condp = (:operation result)

                :started
                (let [{:keys [timestamp]} result]
                  (tracef "[state-process]   updating job %s as :started" job-id)
                  (swap! server-state-ref update-in [:state :pending] (fn [pending-jobs]
                                                                        (->> pending-jobs
                                                                             (mapv (fn [{pending-job-id :job-id :as pending-job}]
                                                                                     (if (= job-id pending-job-id)
                                                                                       (assoc pending-job :start-time (str timestamp))
                                                                                       pending-job)))))))

                :completed
                (let [{:keys [job-status]} result]
                  (tracef "[state-process]   completing job %s as %s" job-id job-status)
                  (cond

                    ;; failure special case for bb.process/shell
                    (and (= job-status :job/failure)
                         (ex-data (:exception result)))
                    (let [update-fn (partial completed-proc-error-update result)]
                      (swap! server-state-ref update :state update-fn))

                    (= job-status :job/failure)
                    (let [update-fn (partial completed-error-update result)]
                      (swap! server-state-ref update :state update-fn))

                    :else
                    (let [update-fn (partial completed-success-update result)]
                      (swap! server-state-ref update :state update-fn)))))
              (tracef "[state-process] handling %s complete" job-id)))]
    (create-process f state-chan admin-chan)))

(defn- handle-job [job-handler request server-state-ref route-prefix]
  (-> request
      (assoc ::server-state server-state-ref)
      (assoc ::route-prefix route-prefix)
      (job-handler)))

(defn- create-input-chan []
  (a/chan max-requests))

(defn- create-server-state-ref []
  (atom {:server {:input-chan (create-input-chan)}
         :state {:pending []
                 :history []}}))

(defn- handle-request [server-state-ref route-prefix handler request]
  (if-let [job-handler (request->handler request route-prefix)]
    (handle-job job-handler request server-state-ref route-prefix)
    (handler request)))

(defn wrap-job-server [handler job-mapping route-prefix stop-chan]
  (try-conform ::job-mapping job-mapping)

  (let [server-state-ref (create-server-state-ref)
        admin-chan (a/mult stop-chan)
        input-chan (get-in @server-state-ref [:server :input-chan])
        worker-chan (a/chan max-jobs)
        state-chan (a/chan)]

    (trace "creating input-process")
    (create-input-process input-chan (a/tap admin-chan (a/chan)) worker-chan)

    (trace "creating worker-process")
    (create-worker-process worker-chan (a/tap admin-chan (a/chan)) state-chan job-mapping)

    (trace "creating state-process")
    (create-state-process state-chan (a/tap admin-chan (a/chan)) server-state-ref)

    (let [route-prefix (if (s/blank? route-prefix) "" route-prefix)]
      (partial handle-request server-state-ref route-prefix handler))))
(ns job-server.middleware.server
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]
            [clojure.string :as s]
            [ring.util.response :as resp]
            [taoensso.timbre :as timbre :refer [trace debug warn error]])
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
  ([content-location body]
   (-> (resp/response body)
       (resp/status 202)
       (resp/header "Content-Location" content-location)))
  ([content-location]
   (accepted content-location nil)))

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

(defn- handle-state-request [{:keys [::server-state] :as _request}]
  (ok (get @server-state :state)))

(defn- handle-history-request [{:keys [::server-state] :as _request}]
  (ok (get-in @server-state [:state :history])))

(defn- handle-jobs [{:keys [params ::server-state] :as request}]
  (let [{job-domain "jobDomain"
         job-type "jobType"} params]
    (if (and job-domain job-type)
      (let [job {:id (random-uuid)
                 :type (keyword job-domain job-type)
                 :params params}
            input-chan (get-in @server-state [:server :input-chan])]
        (if (a/offer! input-chan job)
          (let [{:keys [scheme server-name server-port]} request
                job-url (format "%s://%s:%s/api/job/status/%s" (name scheme) server-name server-port (:id job))]
            (accepted job-url {"jobId" (:id job)}))
          (do
            (warn "failed to enqueue job; jobDomain=%s; jobType=%s" job-domain job-type)
            (server-busy))))
      (bad-request (merge {:job-domain job-domain
                           :job-type job-type}
                          {:message "jobDomain and jobType are required"})))))

(defn- handle-job-status-request [{:keys [uri ::server-state] :as _request}]
  (let [tokens (s/split uri #"/")
        job-id (last tokens)
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
                       :api-job-status {:get #'handle-job-status-request}
                       :api-jobs {:post #'handle-jobs}})

(defn- router [uri route-prefix]
  (cond
    (= uri (str route-prefix "/state"))
    :api-state

    (= uri (str route-prefix "/history"))
    :api-history

    (.startsWith uri (str route-prefix "/job/status"))
    :api-job-status

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

(defn- create-input-process [input-chan req-chan state-chan]
  (a/go-loop []
    (when-some [job (a/<! input-chan)]
      (let [{job-id :id
             job-type :type} job]
        (a/>! state-chan {:operation :input
                          :job-id job-id
                          :job-type job-type
                          :timestamp (LocalDateTime/now)})
        (a/>! req-chan job))
      (recur))))

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

(defn- create-worker-process [worker-chan worker-cancel-chan state-chan job-mapping]
  (a/go-loop []
    (let [[work source-chan] (a/alts! [worker-cancel-chan worker-chan] :priority true)]
      (when (= worker-chan source-chan)
        (let [{:keys [id type params]} work]
          ;; update job state to :started
          (a/>! state-chan {:operation :started
                            :job-id id
                            :timestamp (LocalDateTime/now)})
          (let [_ (trace "destructure job mapping")
                {:keys [extract-args executor]} (get job-mapping type)
                _ (trace "extracting args")
                args (try (extract-args params) (catch Exception _ {}))
                _ (trace "building job")
                job {:id id
                     :executor executor
                     :args args}

                _ (trace "doing job")
                _ (trace job)

                ;; run job
                result (a/<! (a/thread (do-job job)))
                _ (trace "job complete")]
            ;; update job state to :completed
            (a/>! state-chan (assoc result
                                    :operation :completed
                                    :timestamp (LocalDateTime/now)))
            (recur)))))))

(defn- create-state-process [state-chan server-state-ref]
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
    (a/go-loop []
      (when-some [{:keys [operation] :as result} (a/<! state-chan)]
        (condp = operation

          :input
          (let [{:keys [job-id job-type timestamp]} result]
            (swap! server-state-ref update-in [:state :pending] conj {:job-id job-id
                                                                      :job-type job-type
                                                                      :accept-time (str timestamp)})
            (recur))

          :started
          (let [{:keys [job-id timestamp]} result]
            (swap! server-state-ref update-in [:state :pending] (fn [pending-jobs]
                                                                  (->> pending-jobs
                                                                       (mapv (fn [{pending-job-id :job-id :as pending-job}]
                                                                               (if (= job-id pending-job-id)
                                                                                 (assoc pending-job :start-time (str timestamp))
                                                                                 pending-job))))))
            (recur))

          :completed
          (let [{:keys [job-status]} result]
            (cond

              ;; failure special case for bb.process/shell
              (and (= job-status :job/failure)
                   (ex-data (:exception result)))
              (let [update-fn (partial completed-proc-error-update result)]
                (swap! server-state-ref update :state update-fn)
                (recur))

              (= job-status :job/failure)
              (let [update-fn (partial completed-error-update result)]
                (swap! server-state-ref update :state update-fn)
                (recur))

              :else
              (let [update-fn (partial completed-success-update result)]
                (swap! server-state-ref update :state update-fn)
                (recur)))))))))

(defn- create-stop-process [stop-chan input-chan worker-cancel-chan state-chan]
  (a/go
    (when-some [_ (a/<! stop-chan)]
      (when input-chan (a/close! input-chan))
      (a/put! worker-cancel-chan :close)
      (a/close! state-chan))))

(defn- handle-job [job-handler request server-state-ref]
  (-> (assoc request ::server-state server-state-ref)
      (job-handler)))

(defn- create-server-state-ref []
  (atom {:server {:input-chan (a/chan max-requests)}
         :state {:pending []
                 :history []}}))

(defn- handle-request [server-state-ref route-prefix handler request]
  (if-let [job-handler (request->handler request route-prefix)]
    (handle-job job-handler request server-state-ref)
    (handler request)))

(defn wrap-job-server [handler job-mapping route-prefix stop-chan]
  (try-conform ::job-mapping job-mapping)

  (let [server-state-ref (create-server-state-ref)

        input-chan (get-in @server-state-ref [:server :input-chan])
        worker-chan (a/chan max-jobs)
        worker-cancel-chan (a/chan)
        state-chan (a/chan)]

    (create-input-process input-chan worker-chan state-chan)
    (create-worker-process worker-chan worker-cancel-chan state-chan job-mapping)
    (create-state-process state-chan server-state-ref)
    (create-stop-process stop-chan input-chan worker-cancel-chan state-chan)

    (let [route-prefix (if (s/blank? route-prefix) "" route-prefix)]
      (partial handle-request server-state-ref route-prefix handler))))
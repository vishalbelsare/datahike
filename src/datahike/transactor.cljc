(ns datahike.transactor
  (:require [datahike.core :as d]
            [datahike-client.api :as dc]
            [superv.async :refer [<?? <??- S thread-try]]
            [taoensso.timbre :as log]
            [clojure.core.async :refer [>!! chan close! promise-chan put!]]))

(defprotocol PTransactor
  ; Send a transaction. Returns a channel that resolves when the transaction finalizes.
  (send-transaction! [_ tx-data tx-fn])
  ; Returns a channel that resolves when the transactor has shut down.
  (shutdown [_])
  (streaming? [_]))

(defrecord LocalTransactor
           [rx-queue rx-thread streaming?]
  PTransactor
  (send-transaction! [_ tx-data tx-fn]
    (let [p (promise-chan)]
      (>!! rx-queue {:tx-data tx-data :callback p :tx-fn tx-fn})
      p))

  (shutdown [_]
    (close! rx-queue)
    rx-thread)
  (streaming? [_] streaming?))

(defn create-rx-thread
  [connection rx-queue update-and-flush-db]
  (thread-try
   S
   (let [resolve-fn (memoize resolve)]
     (loop []
       (if-let [{:keys [tx-data callback tx-fn]} (<??- rx-queue)]
         (do
           (let [update-fn (resolve-fn tx-fn)
                 tx-report (try (update-and-flush-db connection tx-data update-fn)
                                 ; Only catch ExceptionInfo here (intentionally rejected transactions).
                                 ; Any other exceptions should crash the transactor and signal the supervisor.
                                (catch clojure.lang.ExceptionInfo e e))]
             (when (some? callback)
               (put! callback tx-report)))
           (recur))
         (do
           (log/debug "Transactor rx thread gracefully closed")))))))

(defmulti create-transactor
  (fn [transactor-config conn update-and-flush-db]
    (or (:backend transactor-config) :local)))

(defmethod create-transactor :local
  [{:keys [rx-buffer-size streaming?]} connection update-and-flush-db]
  (let [rx-queue (chan rx-buffer-size)
        rx-thread (create-rx-thread connection rx-queue update-and-flush-db)]
    (map->LocalTransactor
     {:rx-queue  rx-queue
      :rx-thread rx-thread
      :streaming? (if (nil? streaming?) true streaming?)})))


;; datahike-server transactor

(defrecord DatahikeServerTransactor [connection client client-config]
  PTransactor
  (send-transaction! [_ tx-data _]
    (let [p (promise-chan)]
      (log/debug "Sending transaction to datahike-server" client-config tx-data)
      (put! p (dc/transact connection {:db-name (:db-name client-config)
                                       :tx-data tx-data}))
      p))
  (shutdown [_]
      ;; TODO shutdown client here
    )
  (streaming? [_] false))

(defmethod create-transactor :datahike-server
  [config _ _]
  (log/debug "Creating datahike-server transactor for " config)
  (let [client-config (:client-config config)
        client (dc/client client-config)
        connection (dc/connect client (:db-name client-config))]
    (->DatahikeServerTransactor connection client client-config)))



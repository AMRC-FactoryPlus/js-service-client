/*
 * Factory+ NodeJS Utilities
 * ConfigDB service interface.
 * Copyright 2022 AMRC.
 *
 * This class is loaded dynamically by service/configdb.js to avoid a
 * required dependency on rxjs.
 */

import * as rx from "rxjs";
import { v4 as uuidv4 } from "uuid";

import * as rxx from "@amrc-factoryplus/rx-util";

import * as UUIDs from "../uuids.js";
import { ServiceError } from "./service-interface.js";

export class ConfigDBWatcher {
    constructor (configdb, device) {
        this.configdb = configdb;
        this.device = device;
        
        this.log = configdb.fplus.debug.bound("cdbwatch");

        this.notify = this._build_notify();
    }

    _handle_notify_ws (ws) {
        const error = rxx.rx(
            rx.fromEvent(ws, "error"),
            rx.tap(e => this.log("Notify WS error: %o", e)));
        const stopped = rx.merge(error, rx.fromEvent(ws, "close"));
        const msgs = rxx.rx(
            rx.fromEvent(ws, "message"),
            rx.map(m => JSON.parse(m.data)),
            rx.tap(v => this.log("Notify update: %o", v)),
            rx.share());
        const send = msg => {
            if (ws.readyState > ws.constructor.OPEN) {
                this.log("Ignoring send to closing WS");
                return;
            }
            this.log("Sending request %o", msg);
            ws.send(JSON.stringify(msg));
        };

        return rxx.rx(
            rx.merge(rx.of([send, msgs]), error),
            rx.finalize(() => {
                this.log("Closing notify WS");
                ws.close();
            }),
            rx.takeUntil(stopped));
    }

    _build_notify () {
        return rxx.rx(
            rx.defer(() => this.configdb.websocket("notify/v2")),
            rx.catchError(e => {
                this.log("Notify WS connect error: %s", e);
                return rx.EMPTY;
            }),
            rx.flatMap(ws => this._handle_notify_ws(ws)),
            rx.endWith(null),
            rx.repeat({ delay: () => rx.timer(5000 + Math.random()*2000) }),
            rx.tap(v => this.log("Notify socket %s", v ? "open" : "closed")),
            rxx.shareLatest(),
            rx.filter(w => w));
    }
    
    /* This accepts a request structure, without a UUID. When the
     * returned seq is subscribed to, it generates a new UUID for the
     * request, sends the request, and returns a sequence of the
     * responses. */
    request (req) {
        return rxx.rx(
            this.notify,
            rx.switchMap(([send, updates]) => {
                const uuid = uuidv4();
                send({ ...req, uuid });
                return rxx.rx(
                    updates,
                    rx.finalize(() => send({ method: "CLOSE", uuid })),
                    rx.filter(u => u.uuid == uuid),
                    rx.tap(u => u.status < 400 || u.status == 410
                        || this.log("Notify error: %s", u.status)),
                    rx.takeWhile(u => u.status < 400),
                    rx.map(u => u.response));
            }));
    }

    /* For now this returns an Observable<undefined>. It would be much
     * more useful to return an Observable<UUID> identifying which
     * object has had its config entry changed but the ConfigDB MQTT
     * interface doesn't currently expose that information. */
    application (uuid) {
        this._app ??= this.device
            .metric("Last_Changed/Application")
            .pipe(rx.share());

        return this._app.pipe(
            rx.filter(u => u == uuid),
            rx.map(u => undefined));
    }

    watch_url (url) {
        return rxx.rx(
            this.request({ method: "WATCH", request: { url } }),
            rx.map(upd => {
                if (upd.status == 200 || upd.status == 201)
                    return upd.body;
                if (upd.status == 404)
                    return null;
                throw new ServiceError(
                    UUIDs.Service.ConfigDB,
                    `Error watching ${url}`,
                    upd.status);
            }));
    }

    watch_config (app, obj) {
        return this.watch_url(`app/${app}/object/${obj}`);
    }

    watch_search (app, query) {
        return this.application(app).pipe(
            rx.startWith(undefined),
            rx.switchMap(() => this.configdb.search({ app, query })),
        );
    }
}

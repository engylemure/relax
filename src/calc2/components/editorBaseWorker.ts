import { Relation } from "db/exec/Relation";
import { deserializeFromParsedObj } from "../utils/worker-serde/deserializer";
import { t } from "../i18n";
import { RANode } from "db/exec/RANode";
import dbExecClasses from "db/exec/classes";
import { getSerializeValueWithClassName } from "../utils/worker-serde/serializer";
import { Table } from "db/exec/Table";

export type WorkerProcessResp<AstT> = { success: null, error: string | Error } | {
  success: {
    ast: AstT,
    root: RANode
  }, error: null
}

/**
 * class to abstract the communication between our main thread and the
 * respective worker to be used on the linter and exec function, this exposes
 * the available operations at it in a async behavior, to offload our main thread.
 */
export class EditorBaseWorker<AstT> {
  resolveById: Record<string, [(resp: any) => void, (err: Error) => void]> = {}
	cachedRelationsByGroupName: Record<string, { [name: string]: Relation }> = {}
	worker: Worker;
  workerConstructor: () => Worker;
	constructor(worker: Worker, workerConstructor: () => Worker) {
		this.worker = worker;
    this.workerConstructor = workerConstructor;
		this.setupHandlers();
	}

	setupHandlers() {
		this.worker.onmessage = this._onMessage.bind(this);
		this.worker.onerror = console.log;
	}

	reinitializeWorker() {
		const oldWorker = this.worker;
		const worker = this.workerConstructor();
		if (oldWorker) {
			oldWorker.postMessage({ type: 'terminated' });
			oldWorker.onmessage = null;
			oldWorker.onerror = null;
			try { oldWorker.terminate(); } catch (err) { console.error(err); }
		}
		this.worker = worker;
		this.setupHandlers();
		const cacheRelations = function () {
			for (const [groupName, relations] of Object.entries(this.cachedRelationsByGroupName)) {
				this.cacheRelations(groupName, relations, true);
			}
		}.bind(this);
		setTimeout(cacheRelations, 0);
	}

	_onMessage(response: MessageEvent<{ id: string } & (WorkerProcessResp<AstT>)>) {
		const { id, ...resp } = response.data
		const [resolveFn, rejectFn] = this.resolveById[id];
		if (resolveFn) {
			if (resp.success) {
				resp.success.root = deserializeFromParsedObj(resp.success.root as any, dbExecClasses, {});
				resolveFn(resp.success);
			} else {
				if (typeof resp.error === "string") {
					rejectFn(new Error(t(resp.error as any)))
				} else {
					rejectFn(resp.error)
				}
			}
			delete this.resolveById[id]
		}
	}

	cacheRelations(groupName: string, relations: { [name: string]: Relation }, force = false) {
		if (this.cachedRelationsByGroupName[groupName] && !force) {
			return
		}
		this.worker.postMessage({
			type: "cacheRelations",
			payload: {
				groupName,
				relations: getSerializeValueWithClassName(relations)
			}
		});
		this.cachedRelationsByGroupName[groupName] = relations;
	}

	async exec(text: string, groupName: string, withResult: boolean, timeoutMs: number | undefined) {
		const id = window.crypto.randomUUID();
		const resolveById = this.resolveById;
		const execPromise = new Promise<{
			ast: AstT,
			root: RANode,
			result: Table | null
		}>((resolve, reject) => {
			resolveById[id] = [resolve, reject];
			this.worker.postMessage({
				type: "exec",
				payload: { text, groupName, id, withResult }
			});
		});
		const timeout = withResult && timeoutMs ? setTimeout(() => {
			this.reinitializeWorker();
			if (resolveById[id]) {
				const [_, reject] = resolveById[id];
				reject(new Error(t('calc.messages.error-query-execution-timeout', { execTimeout: (timeoutMs / 1000).toLocaleString(undefined, { style: 'unit', unit: 'second', unitDisplay: 'narrow' }) })));
			}
		}, timeoutMs) : undefined;
		return execPromise.then((response) => {
			clearTimeout(timeout);
			return response;
		});
	}
}
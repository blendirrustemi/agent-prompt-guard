// Run the browser submission logic against a small DOM fixture, without a browser dependency.
const { test } = require("node:test");
const assert = require("node:assert/strict");
const { readFileSync } = require("node:fs");
const { randomUUID } = require("node:crypto");
const vm = require("node:vm");
const path = require("node:path");

function setup() {
    class Element {
        constructor() {
            this.value = "";
            this.children = [];
            this.handlers = {};
            this.dataset = {};
            this.style = {};
            this.isConnected = true;
            this.scrollHeight = 40;
            const classes = new Set();
            this.classList = {
                add: (name) => classes.add(name),
                contains: (name) => classes.has(name),
                toggle: (name, enabled) => enabled ? classes.add(name) : classes.delete(name),
            };
        }
        append(...children) { this.children.push(...children); }
        setAttribute() {}
        removeAttribute() {}
        focus() {}
        getBoundingClientRect() { return { left: 0, top: 0 }; }
        addEventListener(name, callback) { this.handlers[name] = callback; }
    }
    const selectors = ["#prompt-form", "#prompt", "#send-button", "#char-count", "#form-error",
        "#messages", "#workspace", ".composer-area", "#intro"];
    const elements = Object.fromEntries(selectors.map((name) => [name, new Element()]));
    elements["#prompt-form"].dataset.maxLength = "8000";
    elements["#prompt-form"].reset = () => { elements["#prompt"].value = ""; };
    const responses = [];
    const requests = [];
    const timers = new Map();
    let timerId = 0;
    const context = {
        document: { querySelector: (name) => elements[name], createElement: () => new Element() },
        crypto: { randomUUID }, AbortController,
        setTimeout: (callback, delay) => { timers.set(++timerId, { callback, delay }); return timerId; },
        clearTimeout: (id) => timers.delete(id),
        fetch: async (url, options = {}) => {
            requests.push({ url, body: options.body && JSON.parse(options.body) });
            const response = responses.shift();
            assert.ok(response, "Unexpected request");
            return { ok: response.code < 400, json: async () => response.body };
        },
    };
    vm.runInNewContext(readFileSync(path.join(__dirname, "../static/app.js"), "utf8"), context);
    return {
        elements, requests, responses,
        type(text) { elements["#prompt"].value = text; elements["#prompt"].handlers.input(); },
        submit() { return elements["#prompt-form"].handlers.submit({ preventDefault() {} }); },
        async poll() {
            const [id, timer] = [...timers].find(([, value]) => value.delay === 2000);
            timers.delete(id);
            await timer.callback();
        },
        status() { return elements["#messages"].children.at(-1).children[1].children[1].textContent; },
    };
}

test("queued prompt stays queued through missing storage and then becomes pending", async () => {
    const app = setup();
    const prompt = { id: randomUUID(), text: "  Hello 🛡️\n", created_at: new Date().toISOString(), decision: "pending" };
    app.type(prompt.text);
    app.responses.push({ code: 202, body: { prompt, status: "queued" } });
    await app.submit();
    assert.equal(app.status(), "Queued");
    assert.equal(app.elements["#prompt"].value, "");
    app.responses.push({ code: 404, body: { error: "Not stored yet" } });
    await app.poll();
    assert.equal(app.status(), "Queued");
    app.responses.push({ code: 503, body: { error: "Storage unavailable" } });
    await app.poll();
    assert.equal(app.status(), "Queued");
    app.responses.push({ code: 200, body: { prompt, status: "stored" } });
    await app.poll();
    assert.equal(app.status(), "Pending check");
});

test("failed submission preserves input and ID, while edited text gets a new ID", async () => {
    const app = setup();
    app.type("Keep this text");
    for (let i = 0; i < 2; i++) {
        app.responses.push({ code: 503, body: { error: "Could not confirm delivery" } });
        await app.submit();
        assert.equal(app.elements["#prompt"].value, "Keep this text");
    }
    assert.equal(app.requests[0].body.prompt_id, app.requests[1].body.prompt_id);
    assert.equal(app.elements["#messages"].children.length, 0);
    app.type("Changed text");
    app.responses.push({ code: 503, body: { error: "Could not confirm delivery" } });
    await app.submit();
    assert.notEqual(app.requests[1].body.prompt_id, app.requests[2].body.prompt_id);
});

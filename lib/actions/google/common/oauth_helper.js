"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.GoogleOAuthHelper = void 0;
const gaxios = require("gaxios");
const googleapis_1 = require("googleapis");
const Hub = require("../../../hub");
const error_utils_1 = require("./error_utils");
class GoogleOAuthHelper {
    /******** Contsructor & public helpers ********/
    constructor(actionInstance, log) {
        this.actionInstance = actionInstance;
        this.log = log;
    }
    makeOAuthClient(redirectUri) {
        return new googleapis_1.google.auth.OAuth2(this.actionInstance.oauthClientId, this.actionInstance.oauthClientSecret, redirectUri);
    }
    async makeLoginForm(request) {
        // Step 0 in the outh flow - generate an *ActionHub* url that user can visit to kick things off
        // The payload is arbitrary state data Google will receive and send back to us with the redirect.
        // In this case it contains the Looker state-update url we will use to persist the tokens when flow is complete.
        // We need to start tracking the url now because Looker makes one-time-use-only endpoints for updating user state
        // and this form request is where we receive it.
        const payloadString = JSON.stringify({ stateUrl: request.params.state_url, webhookId: request.webhookId });
        //  Payload is encrypted to keep things private and prevent tampering
        let encryptedPayload;
        try {
            const actionCrypto = new Hub.ActionCrypto();
            encryptedPayload = await actionCrypto.encrypt(payloadString);
        }
        catch (e) {
            this.log("error", "Payload encryption error:", e.toString());
            throw e;
        }
        // Step 1 in the oauth flow - user clicks the button in the form and visits the AH url generated here.
        // That response will be auto handled by the AH server as a redirect to the result of oauthUrl function below.
        const startAuthUrl = `${process.env.ACTION_HUB_BASE_URL}/actions/${this.actionInstance.name}/oauth?state=${encryptedPayload}`;
        this.log("debug", "login form has startAuthUrl=", startAuthUrl);
        const form = new Hub.ActionForm();
        form.state = new Hub.ActionState();
        form.state.data = "reset";
        form.fields = [];
        form.fields.push({
            name: "login",
            type: "oauth_link_google",
            label: "Log in",
            description: "In order to send to this destination, you will need to log in"
                + " once to your Google account.",
            oauth_url: startAuthUrl,
        });
        return form;
    }
    /******** Handlers for Hub.OAuthAction endpoints ********/
    async oauthUrl(redirectUri, encryptedPayload) {
        // Step 2 of the oauth flow - user will be sent to this Google url to consent to the login.
        // `redirectUri` in this case is the AH url to which Google will send them *back*, along with an auth code.
        // Note the "payload" is what we generated in loginForm above and will just be passed back to us.
        const oauthClient = this.makeOAuthClient(redirectUri);
        this.log("debug", "beginning oauth flow with redirect url:", redirectUri);
        const url = oauthClient.generateAuthUrl({
            access_type: "offline",
            scope: this.actionInstance.oauthScopes,
            prompt: "select_account",
            state: encryptedPayload,
        });
        this.log("debug", "generated Google auth url:", url);
        return url;
    }
    async oauthFetchInfo(urlParams, redirectUri) {
        // Step 3 (final!) of the oauth flow
        // This method is called after Google sends the user back to us.
        // Request url contains the encrypted payload we sent at the start.
        let plaintext;
        try {
            const actionCrypto = new Hub.ActionCrypto();
            plaintext = await actionCrypto.decrypt(urlParams.state);
        }
        catch (err) {
            this.log("error", "Encryption not correctly configured: ", err.toString());
            throw err;
        }
        // Request url also contains the auth code which we can use to get the actual user tokens.
        // Under the hood the google-auth-library is making http calls to do this for us.
        const { tokens } = await this.makeOAuthClient(redirectUri).getToken(urlParams.code);
        // The payload was serialized as json (see loginForm above)
        // So far the only prop it contains is the Looker URL that lets us set the user's state in Looker.
        const payload = JSON.parse(plaintext);
        const webhookId = payload.webhookId ? payload.webhookId : "no-id";
        // If the payload does ever contain something besides the stateUrl, we would likely need to add that here
        const userState = { tokens, redirect: redirectUri };
        // So now we use that state url to persist the oauth tokens
        try {
            await gaxios.request({
                method: "POST",
                url: payload.stateUrl,
                data: userState,
            });
        }
        catch (err) {
            // We have seen weird behavior where Looker correctly updates the state, but returns a nonsense status code
            if (err instanceof gaxios.GaxiosError && err.response !== undefined && err.response.status < 100) {
                this.log("warn", `[${webhookId}] Ignoring state update response with response code ${err.response.status}`);
            }
            else {
                this.log("error", `[${webhookId}] Error sending user state to Looker:`, err.toString());
                (0, error_utils_1.sanitizeError)(err);
                throw err;
            }
        }
        this.log("info", `[${webhookId}] OAuth login flow complete`);
    }
    /******** Helper for ad hoc token refresh ********/
    async refreshAccessToken(currentTokens) {
        const oauthClient = this.actionInstance.makeOAuthClient();
        oauthClient.setCredentials(currentTokens);
        const getTokenResp = await oauthClient.getAccessToken();
        if (getTokenResp.res == null || getTokenResp.res.data == null) {
            return undefined;
        }
        return getTokenResp.res.data;
    }
}
exports.GoogleOAuthHelper = GoogleOAuthHelper;

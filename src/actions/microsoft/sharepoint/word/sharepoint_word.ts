import { AuthenticationProvider, Client } from "@microsoft/microsoft-graph-client"
import * as parse from "csv-parse"
import "isomorphic-fetch"
import * as winston from "winston"
import { HTTP_ERROR } from "../../../../error_types/http_errors"
import { getHttpErrorType } from "../../../../error_types/utils"
import * as Hub from "../../../../hub"
import { Error, errorWith } from "../../../../hub"

const MAX_RETRY_COUNT = 5
const RETRY_BASE_DELAY = process.env.SHAREPOINT_WORD_BASE_DELAY ? Number(process.env.SHAREPOINT_WORD_BASE_DELAY) : 3
const LOG_PREFIX = "[SHAREPOINT_WORD]"
const RETRIABLE_CODES = [429, 409, 500, 504, 503]
const MAX_REQUEST_BATCH = process.env.SHAREPOINT_WORD_WRITE_BATCH ? Number(process.env.SHAREPOINT_WORD_WRITE_BATCH) : 100

class CustomAuthProvider implements AuthenticationProvider {
    private tokens: any

    constructor(tokens: any) {
        this.tokens = tokens
    }

    async getAccessToken(): Promise<string> {
        return this.tokens.access_token
    }
}

export class SharePointWordAction extends Hub.Action {
    name = "sharepoint_word"
    label = "SharePoint Word"
    iconName = "microsoft/sharepoint/word/word.svg"
    description = "Create a new Word document with data in a table."
    supportedActionTypes = [Hub.ActionType.Query]
    supportedFormats = [Hub.ActionFormat.Csv]
    requiredFields = []
    params = []
    mimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

    async execute(request: Hub.ActionRequest) {
        const resp = new Hub.ActionResponse()

        if (!request.params.state_json) {
            winston.info("No state json found", { webhookId: request.webhookId })
            resp.success = false
            resp.message = "No state found with oauth credentials."
            resp.state = new Hub.ActionState()
            resp.state.data = "reset"
            return resp
        }

        const stateJson = JSON.parse(request.params.state_json)

        if (stateJson.tokens) {
            try {
                const client = Client.init({
                    authProvider: new CustomAuthProvider(stateJson.tokens),
                })

                let filename = request.formParams.filename || request.suggestedFilename()
                if (!filename) {
                    const error: Hub.Error = Hub.errorWith(
                        HTTP_ERROR.bad_request,
                        `${LOG_PREFIX} Error creating file name`,
                    )
                    resp.error = error
                    resp.success = false
                    resp.message = error.message
                    resp.webhookId = request.webhookId
                    winston.error(`${error.message}`, { error, webhookId: request.webhookId })
                    return resp
                }

                await this.createWordDocWithTable(filename, request, client)
                resp.success = true
            } catch (e: any) {
                const errorType = getHttpErrorType(e, this.name)
                let error: Error = errorWith(
                    errorType,
                    `${LOG_PREFIX} ${e.toString()}`,
                )

                if (e.statusCode && e.message) {
                    error = { ...error, http_code: e.statusCode, message: `${errorType.description} ${LOG_PREFIX} ${e.message}` }
                    resp.message = e.message
                } else {
                    resp.message = e.toString()
                }

                resp.success = false
                resp.webhookId = request.webhookId
                resp.error = error
                winston.error(`${error.message}`, { error, webhookId: request.webhookId })
            }
        } else {
            winston.info("Request did not have oauth tokens present", { webhookId: request.webhookId })
            resp.success = false
            resp.message = "Request did not have necessary oauth tokens saved. Fast failing"
            resp.state = new Hub.ActionState()
            resp.state.data = "reset"
        }
        return resp
    }

    async form(request: Hub.ActionRequest) {
        const form = new Hub.ActionForm()
        form.fields = [{
            label: "Filename",
            name: "filename",
            required: true,
            type: "string",
        }, {
            label: "SharePoint Site",
            name: "site",
            required: true,
            type: "string",
        }, {
            label: "Document Library",
            name: "library",
            required: true,
            type: "string",
        }]
        return form
    }

    protected async delay(time: number) {
        await new Promise<void>((resolve) => {
            setTimeout(resolve, time)
        })
    }

    private async createWordDocWithTable(filename: string, request: Hub.ActionRequest, client: Client) {
        return new Promise<void>((resolve, reject) => {
            const rows: string[][] = []
            const csvparser = parse({
                rtrim: true,
                ltrim: true,
                bom: true,
                relax_column_count: true,
            })

            csvparser.on("data", (line: string[]) => {
                rows.push(line)
            })

            csvparser.on("end", async () => {
                try {
                    if (rows.length === 0) {
                        throw new Error("No data to insert")
                    }

                    const headers = rows[0]

                    // Create a new Word document using Microsoft Graph API
                    const site = request.formParams.site
                    const library = request.formParams.library

                    // Convert the data to Office Open XML format
                    const documentContent = this.generateWordDocument(rows)

                    // Upload to SharePoint
                    await this.retriableUpload(
                        client,
                        site,
                        library,
                        this.sanitizeFilename(filename),
                        documentContent,
                        0,
                        request.webhookId!
                    )

                    resolve()
                } catch (e) {
                    reject(e)
                }
            })

            csvparser.on("error", (e) => {
                reject(e)
            })

            request.stream(async (readable) => {
                readable.pipe(csvparser)
                return Promise.resolve()
            })
        })
    }

    private generateWordDocument(rows: string[][]): string {
        // This is a simplified example - in practice, you would generate proper Office Open XML
        const headers = rows[0]
        let content = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        content += '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        content += '<w:body><w:tbl>'

        // Add headers
        content += '<w:tr>'
        for (const header of headers) {
            content += `<w:tc><w:p><w:r><w:t>${header}</w:t></w:r></w:p></w:tc>`
        }
        content += '</w:tr>'

        // Add data rows
        for (let i = 1; i < rows.length; i++) {
            content += '<w:tr>'
            for (const cell of rows[i]) {
                content += `<w:tc><w:p><w:r><w:t>${cell}</w:t></w:r></w:p></w:tc>`
            }
            content += '</w:tr>'
        }

        content += '</w:tbl></w:body></w:document>'
        return content
    }

    private async retriableUpload(
        client: Client,
        site: string,
        library: string,
        filename: string,
        content: string,
        attempt: number,
        webhookId: string
    ): Promise<any> {
        try {
            return await client.api(`/sites/${site}/drives/${library}/root:/${filename}:/content`)
                .put(content)
        } catch (e: any) {
            winston.debug(`Document upload error: ${e}`, { webhookId })
            if (RETRIABLE_CODES.includes(e.statusCode) && process.env.SHAREPOINT_WORD_RETRY && attempt <= MAX_RETRY_COUNT) {
                winston.warn("Queueing retry for document upload", { webhookId })
                await this.delay((RETRY_BASE_DELAY ** (attempt)) * 1000)
                return this.retriableUpload(client, site, library, filename, content, attempt + 1, webhookId)
            } else {
                throw e
            }
        }
    }

    private sanitizeFilename(filename: string) {
        return filename.split("'").join("\'")
    }
}

if (process.env.SHAREPOINT_CLIENT_ID && process.env.SHAREPOINT_CLIENT_SECRET) {
    Hub.addAction(new SharePointWordAction())
} else {
    winston.warn(
        `${LOG_PREFIX} Action not registered because required environment variables are missing.`,
    )
} 
import { HTTP_ERROR } from "../../../../error_types/http_errors"
import * as Hub from "../../../../hub"

import * as parse from "csv-parse"
import { Credentials } from "google-auth-library"
import { docs_v1, drive_v3, google } from "googleapis"
import * as winston from "winston"
import { getHttpErrorType } from "../../../../error_types/utils"
import { Error, errorWith } from "../../../../hub"
import { GoogleDriveAction } from "../google_drive"
import Drive = drive_v3.Drive
import Docs = docs_v1.Docs

const MAX_RETRY_COUNT = 5
const RETRY_BASE_DELAY = process.env.GOOGLE_DOCS_BASE_DELAY ? Number(process.env.GOOGLE_DOCS_BASE_DELAY) : 3
const LOG_PREFIX = "[GOOGLE_DOCS]"
const ROOT = "root"
const FOLDERID_REGEX = /\/folders\/(?<folderId>[^\/?]+)/
const RETRIABLE_CODES = [429, 409, 500, 504, 503]
const MAX_REQUEST_BATCH = process.env.GOOGLE_DOCS_WRITE_BATCH ? Number(process.env.GOOGLE_DOCS_WRITE_BATCH) : 100

export class GoogleDocsAction extends GoogleDriveAction {
    name = "google_docs"
    label = "Google Docs"
    iconName = "google/drive/docs/docs.svg"
    description = "Create a new Google Doc with data in a table."
    supportedActionTypes = [Hub.ActionType.Query]
    supportedFormats = [Hub.ActionFormat.Csv]
    executeInOwnProcess = true
    mimeType = "application/vnd.google-apps.document"

    async execute(request: Hub.ActionRequest) {
        const resp = new Hub.ActionResponse()

        if (!request.params.state_json) {
            winston.info("No state json found", {webhookId: request.webhookId})
            resp.success = false
            resp.message = "No state found with oauth credentials."
            resp.state = new Hub.ActionState()
            resp.state.data = "reset"
            return resp
        }

        const stateJson = JSON.parse(request.params.state_json)

        if (stateJson.tokens && stateJson.redirect) {
            await this.validateUserInDomainAllowlist(request.params.domain_allowlist,
                                                   stateJson.redirect,
                                                   stateJson.tokens,
                                                   request.webhookId)
                .catch((error) => {
                    winston.info(error + " - invalidating token", {webhookId: request.webhookId})
                    resp.success = false
                    resp.state = new Hub.ActionState()
                    resp.message = "User Domain validation failed"
                    resp.state.data = "reset"
                    return resp
                })

            const drive = await this.driveClientFromRequest(stateJson.redirect, stateJson.tokens)
            const docs = await this.docsClientFromRequest(stateJson.redirect, stateJson.tokens)

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
                winston.error(`${error.message}`, {error, webhookId: request.webhookId})
                return resp
            }

            try {
                await this.createDocWithTable(filename, request, drive, docs)
                resp.success = true
            } catch (e: any) {
                this.sanitizeGaxiosError(e)

                const errorType = getHttpErrorType(e, this.name)
                let error: Error = errorWith(
                    errorType,
                    `${LOG_PREFIX} ${e.toString()}`,
                )

                if (e.code && e.errors && e.errors[0] && e.errors[0].message) {
                    error = {...error, http_code: e.code, message: `${errorType.description} ${LOG_PREFIX} ${e.errors[0].message}`}
                    resp.message = e.errors[0].message
                } else {
                    resp.message = e.toString()
                }

                resp.success = false
                resp.webhookId = request.webhookId
                resp.error = error
                winston.error(`${error.message}`, {error, webhookId: request.webhookId})
            }
        } else {
            winston.info("Request did not have oauth tokens present", {webhookId: request.webhookId})
            resp.success = false
            resp.message = "Request did not have necessary oauth tokens saved. Fast failing"
            resp.state = new Hub.ActionState()
            resp.state.data = "reset"
        }
        return resp
    }

    async form(request: Hub.ActionRequest) {
        const form = await super.form(request)
        return form
    }

    async oauthUrl(redirectUri: string, encryptedState: string) {
        const oauth2Client = this.oauth2Client(redirectUri)

        // generate a url that asks permissions for Google Drive and Docs scope
        const scopes = [
            "https://www.googleapis.com/auth/documents",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/userinfo.email",
        ]

        const url = oauth2Client.generateAuthUrl({
            access_type: "offline",
            scope: scopes,
            prompt: "consent",
            state: encryptedState,
        })
        return url.toString()
    }

    protected async delay(time: number) {
        await new Promise<void>((resolve) => {
            setTimeout(resolve, time)
        })
    }

    protected async docsClientFromRequest(redirect: string, tokens: Credentials) {
        const client = this.oauth2Client(redirect)
        client.setCredentials(tokens)
        return google.docs({version: "v1", auth: client})
    }

    private async createDocWithTable(filename: string, request: Hub.ActionRequest, drive: Drive, docs: Docs) {
        let folder: string | undefined
        if (request.formParams.folderid) {
            if (request.formParams.folderid.includes("my-drive")) {
                folder = ROOT
            } else {
                const match = request.formParams.folderid.match(FOLDERID_REGEX)
                if (match && match.groups) {
                    folder = match.groups.folderId
                } else {
                    folder = ROOT
                }
            }
        } else {
            folder = request.formParams.folder
        }

        // First create an empty document
        const fileMetadata: drive_v3.Schema$File = {
            name: this.sanitizeFilename(filename),
            mimeType: this.mimeType,
            parents: folder ? [folder] : undefined,
        }

        const driveParams: drive_v3.Params$Resource$Files$Create = {
            requestBody: fileMetadata,
            fields: "id",
        }

        if (request.formParams.drive !== undefined && request.formParams.drive !== "mydrive") {
            driveParams.requestBody!.driveId! = request.formParams.drive
            driveParams.supportsAllDrives = true
        }

        const file = await drive.files.create(driveParams)
        const documentId = file.data.id

        if (!documentId) {
            throw new Error("Failed to create document")
        }

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

                    // Create table with headers
                    const requests: docs_v1.Schema$Request[] = [
                        // Set landscape orientation
                        {
                            updateDocumentStyle: {
                                documentStyle: {
                                    pageSize: {
                                        width: {
                                            magnitude: 11,
                                            unit: "IN"
                                        },
                                        height: {
                                            magnitude: 8.5,
                                            unit: "IN"
                                        }
                                    }
                                },
                                fields: "pageSize"
                            }
                        },
                        // Create table
                        {
                            insertTable: {
                                rows: rows.length,
                                columns: headers.length,
                                location: {
                                    index: 1,
                                }
                            }
                        }
                    ]

                    // Style the header row
                    requests.push({
                        updateTableCellStyle: {
                            tableCellStyle: {
                                backgroundColor: {
                                    color: {
                                        rgbColor: {
                                            red: 0.95,
                                            green: 0.95,
                                            blue: 0.95
                                        }
                                    }
                                }
                            },
                            fields: "backgroundColor",
                            tableRange: {
                                tableCellLocation: {
                                    rowIndex: 0,
                                    columnIndex: 0
                                },
                                rowSpan: 1,
                                columnSpan: headers.length
                            }
                        }
                    })

                    // Make headers bold
                    for (let i = 0; i < headers.length; i++) {
                        requests.push({
                            updateTextStyle: {
                                textStyle: {
                                    bold: true
                                },
                                range: {
                                    startIndex: this.getTableCellLocation(0, i, headers.length),
                                    endIndex: this.getTableCellLocation(0, i, headers.length) + headers[i].length
                                },
                                fields: "bold"
                            }
                        })
                    }

                    // Insert the data
                    let currentIndex = 1
                    const batchedRequests: docs_v1.Schema$Request[][] = [[...requests]]
                    let currentBatch = 0

                    for (let row = 0; row < rows.length; row++) {
                        for (let col = 0; col < rows[row].length; col++) {
                            const cellText = rows[row][col]
                            const insertRequest = {
                                insertText: {
                                    text: cellText,
                                    location: {
                                        index: this.getTableCellLocation(row, col, headers.length)
                                    }
                                }
                            }

                            if (batchedRequests[currentBatch].length >= MAX_REQUEST_BATCH) {
                                currentBatch++
                                batchedRequests[currentBatch] = []
                            }
                            batchedRequests[currentBatch].push(insertRequest)
                            currentIndex += cellText.length
                        }
                    }

                    // Apply the changes in batches
                    for (const batch of batchedRequests) {
                        await this.retriableDocumentUpdate(documentId, docs, batch, 0, request.webhookId!)
                    }

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

    private async retriableDocumentUpdate(documentId: string, docs: Docs, requests: docs_v1.Schema$Request[], attempt: number, webhookId: string): Promise<any> {
        return docs.documents.batchUpdate({
            documentId,
            requestBody: {
                requests
            }
        }).catch(async (e: any) => {
            this.sanitizeGaxiosError(e)
            winston.debug(`Document update error: ${e}`, {webhookId})
            if (RETRIABLE_CODES.includes(e.code) && process.env.GOOGLE_DOCS_RETRY && attempt <= MAX_RETRY_COUNT) {
                winston.warn("Queueing retry for document update", {webhookId})
                await this.delay((RETRY_BASE_DELAY ** (attempt)) * 1000)
                // Try again and increment attempt
                return this.retriableDocumentUpdate(documentId, docs, requests, attempt + 1, webhookId)
            } else {
                throw e
            }
        })
    }

    private async retriableFileList(drive: Drive, options: any, attempt: number, webhookId: string): Promise<any> {
        return await drive.files.list(options).catch(async (e: any) => {
            this.sanitizeGaxiosError(e)
            winston.debug(`File list error: ${e}`, {webhookId})
            if (RETRIABLE_CODES.includes(e.code) && process.env.GOOGLE_DOCS_RETRY && attempt <= MAX_RETRY_COUNT) {
                winston.warn("Queueing retry for file list", {webhookId})
                await this.delay((RETRY_BASE_DELAY ** (attempt)) * 1000)
                // Try again and increment attempt
                return this.retriableFileList(drive, options, attempt + 1, webhookId)
            } else {
                throw e
            }
        })
    }

    private getTableCellLocation(row: number, col: number, numColumns: number): number {
        // Each cell has a newline character, so we need to account for that in the index calculation
        // The +1 at the start is for the initial newline before the table
        return 1 + (row * numColumns + col)
    }

    sanitizeFilename(filename: string) {
        return filename.split("'").join("\'")
    }
}

if (process.env.GOOGLE_DOCS_CLIENT_ID && process.env.GOOGLE_DOCS_CLIENT_SECRET) {
    Hub.addAction(new GoogleDocsAction())
} 
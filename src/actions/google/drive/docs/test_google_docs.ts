import * as b64 from "base64-url"
import * as chai from "chai"
import * as sinon from "sinon"

import concatStream = require("concat-stream")

import * as Hub from "../../../../hub"

import { ActionCrypto } from "../../../../hub"
import { GoogleDocsAction } from "./google_docs"

const action = new GoogleDocsAction()
action.executeInOwnProcess = false

const stubFileName = "stubSuggestedFilename"
const stubFolder = "stubSuggestedFolder"
const stubDocId = "doc123"
const MAX_RETRIES = 5 // Match MAX_RETRY_COUNT from GoogleDocsAction

function expectGoogleDocsMatch(request: Hub.ActionRequest, paramsMatch: any) {
  const expectedBuffer = paramsMatch.media.body
  delete paramsMatch.media.body

  const createSpy = sinon.spy(async (params: any) => {
    params.media.body.pipe(concatStream((buffer) => {
      chai.expect(buffer.toString()).to.equal(expectedBuffer.toString())
    }))
    return { data: { id: stubDocId } }
  })

  const stubClient = sinon.stub(action as any, "driveClientFromRequest")
    .resolves({
      files: {
        create: createSpy,
      },
    })

  return chai.expect(action.validateAndExecute(request)).to.be.fulfilled.then(() => {
    chai.expect(createSpy).to.have.been.called
    chai.expect(createSpy).to.have.been.calledWith(sinon.match({
      requestBody: {
        name: paramsMatch.requestBody.name,
        mimeType: paramsMatch.requestBody.mimeType,
        parents: paramsMatch.requestBody.parents,
      }
    }))
    stubClient.restore()
  })
}

describe(`${action.constructor.name} unit tests`, () => {
  let encryptStub: any
  let decryptStub: any

  beforeEach(() => {
    encryptStub = sinon.stub(ActionCrypto.prototype, "encrypt").callsFake(async (s: string) => b64.encode(s))
    decryptStub = sinon.stub(ActionCrypto.prototype, "decrypt").callsFake(async (s: string) => b64.decode(s))
  })

  afterEach(() => {
    encryptStub.restore()
    decryptStub.restore()
  })

  describe("action", () => {
    describe("execute", () => {
      it("successfully interprets execute request params", () => {
        const request = new Hub.ActionRequest()
        const dataBuffer = Buffer.from("col1,col2\nval1,val2")
        request.type = Hub.ActionType.Query
        request.attachment = { dataBuffer, fileExtension: "csv" }
        request.formParams = { filename: stubFileName, folder: stubFolder }
        request.params = {
          state_url: "https://looker.state.url.com/action_hub_state/asdfasdfasdfasdf",
          state_json: JSON.stringify({ tokens: "access", redirect: "url" }),
        }
        return expectGoogleDocsMatch(request, {
          requestBody: {
            name: action.sanitizeFilename(stubFileName),
            mimeType: "application/vnd.google-apps.document",
            parents: [stubFolder],
          },
          media: {
            body: dataBuffer,
          },
        })
      })

      it("creates document with table from CSV data and handles batching", (done) => {
        const stubDriveClient = sinon.stub(action as any, "driveClientFromRequest")
          .resolves({
            files: {
              create: async () => Promise.resolve({
                data: {
                  id: stubDocId,
                },
              }),
            },
          })

        const batchUpdateSpy = sinon.spy(async (params: any) => {
          // Verify the table creation request
          const requests = params.requestBody.requests
          chai.expect(requests).to.have.length.at.most(100) // MAX_REQUEST_BATCH
          
          return Promise.resolve({})
        })

        const stubDocsClient = sinon.stub(action as any, "docsClientFromRequest")
          .resolves({
            documents: {
              batchUpdate: batchUpdateSpy,
            },
          })

        // Create a larger CSV file that will require batching
        const headers = Array(10).fill(0).map((_, i) => `col${i}`).join(",")
        const rows = Array(20).fill(headers).join("\n")
        const csvFile = `${headers}\n${rows}`

        const request = new Hub.ActionRequest()
        request.attachment = { dataBuffer: Buffer.from(csvFile), fileExtension: "csv" }
        request.formParams = { filename: "test_doc", folder: "folder" }
        request.type = Hub.ActionType.Query
        request.params = {
          state_url: "https://looker.state.url.com/action_hub_state/asdfasdfasdfasdf",
          state_json: `{"tokens": {"access_token": "token"}, "redirect": "fake.com"}`,
        }

        chai.expect(action.validateAndExecute(request)).to.eventually.be.fulfilled.then(() => {
          chai.expect(batchUpdateSpy).to.have.been.called
          chai.expect(batchUpdateSpy.callCount).to.be.greaterThan(1) // Should have multiple batches
          stubDriveClient.restore()
          stubDocsClient.restore()
          done()
        })
      })

      it("handles empty data gracefully", (done) => {
        const stubDriveClient = sinon.stub(action as any, "driveClientFromRequest")
          .resolves({
            files: {
              create: async () => Promise.resolve({
                data: {
                  id: stubDocId,
                },
              }),
            },
          })

        const stubDocsClient = sinon.stub(action as any, "docsClientFromRequest")
          .resolves({
            documents: {
              batchUpdate: async () => Promise.resolve({}),
            },
          })

        const request = new Hub.ActionRequest()
        request.attachment = { dataBuffer: Buffer.from(""), fileExtension: "csv" }
        request.formParams = { filename: "test_doc", folder: "folder" }
        request.type = Hub.ActionType.Query
        request.params = {
          state_url: "https://looker.state.url.com/action_hub_state/asdfasdfasdfasdf",
          state_json: `{"tokens": {"access_token": "token"}, "redirect": "fake.com"}`,
        }

        chai.expect(action.validateAndExecute(request)).to.eventually.have.property("success", false)
          .then(() => {
            stubDriveClient.restore()
            stubDocsClient.restore()
            done()
          })
      })

      it("handles API errors appropriately", (done) => {
        const stubDriveClient = sinon.stub(action as any, "driveClientFromRequest")
          .resolves({
            files: {
              create: async () => Promise.reject({
                code: 1234,
                errors: [
                  {
                    message: "testException",
                  },
                ],
              }),
            },
          })

        const request = new Hub.ActionRequest()
        request.attachment = { dataBuffer: Buffer.from("col1,col2\nval1,val2"), fileExtension: "csv" }
        request.formParams = { filename: "test_doc", folder: "folder" }
        request.type = Hub.ActionType.Query
        request.params = {
          state_json: `{"tokens": {"access_token": "token"}, "redirect": "fake.com"}`,
        }
        request.webhookId = "webhookId"

        chai.expect(action.validateAndExecute(request)).to.eventually.deep.equal({
          success: false,
          message: "testException",
          refreshQuery: false,
          validationErrors: [],
          error: {
            documentation_url: "TODO",
            http_code: 1234,
            location: "ActionContainer",
            message: "Internal server error. [GOOGLE_DOCS] testException",
            status_code: "INTERNAL",
          },
          webhookId: "webhookId",
        }).and.notify(stubDriveClient.restore).and.notify(done)
      })

      it("handles missing filename", (done) => {
        const request = new TestActionRequest()
        request.webhookId = "webhookId"
        request.type = Hub.ActionType.Query
        request.attachment = { dataBuffer: Buffer.from("data"), fileExtension: "csv" }
        request.params = {
          state_json: `{"tokens": {"access_token": "token"}, "redirect": "fake.com"}`,
        }

        chai.expect(action.validateAndExecute(request)).to.eventually.deep.equal({
          success: false,
          message: "Server cannot process request due to client request error. [GOOGLE_DOCS] Error creating file name",
          refreshQuery: false,
          validationErrors: [],
          error: {
            documentation_url: "TODO",
            http_code: 400,
            location: "ActionContainer",
            message: "Server cannot process request due to client request error. [GOOGLE_DOCS] Error creating file name",
            status_code: "BAD_REQUEST",
          },
          webhookId: "webhookId",
        }).and.notify(done)
      })
    })

    describe("retriableDocumentUpdate", () => {
      it("will retry if a 429 code is received", (done) => {
        const delayStub = sinon.stub(action as any, "delay")
        process.env.GOOGLE_DOCS_RETRY = "true"

        const batchUpdateStub = sinon.stub().rejects({ code: 429 })
        const docs = {
          documents: {
            batchUpdate: batchUpdateStub
          }
        }

        // @ts-ignore
        chai.expect((action as any).retriableDocumentUpdate("docId", docs, [], 0, "webhookId"))
          .to.eventually.be.rejected.then(() => {
            chai.expect(batchUpdateStub.callCount).to.equal(MAX_RETRIES + 1)
            chai.expect(delayStub).to.have.been.calledWith(3000)
            chai.expect(delayStub).to.have.been.calledWith(9000)
            chai.expect(delayStub).to.have.been.calledWith(27000)
            delayStub.restore()
            done()
          })
      })

      it("will not retry if GOOGLE_DOCS_RETRY is not set", (done) => {
        const delayStub = sinon.stub(action as any, "delay")
        process.env.GOOGLE_DOCS_RETRY = ""

        const batchUpdateStub = sinon.stub().rejects({ code: 429 })
        const docs = {
          documents: {
            batchUpdate: batchUpdateStub
          }
        }

        // @ts-ignore
        chai.expect(action.retriableDocumentUpdate("docId", docs, [], 0, "webhookId"))
          .to.eventually.be.rejected.then(() => {
            chai.expect(batchUpdateStub.callCount).to.equal(1)
            chai.expect(delayStub).to.not.have.been.called
            delayStub.restore()
            done()
          })
      })
    })

    describe("form", () => {
      it("adds drive selection options", (done) => {
        const stubClient = sinon.stub(action as any, "driveClientFromRequest")
          .resolves({
            files: {
              list: async () => Promise.resolve({
                data: {
                  files: [
                    {
                      id: "fake_id",
                      name: "fake_name",
                    },
                  ],
                },
              }),
            },
            drives: {
              list: async () => Promise.resolve({
                data: {
                  drives: [
                    {
                      id: "fake_drive",
                      name: "fake_drive_label",
                    },
                  ],
                },
              }),
            },
          })

        const request = new Hub.ActionRequest()
        request.params = {
          state_url: "https://looker.state.url.com/action_hub_state/asdfasdfasdfasdf",
          state_json: JSON.stringify({ tokens: "access", redirect: "url" }),
        }

        const form = action.validateAndFetchForm(request)
        chai.expect(form).to.eventually.deep.equal({
          fields: [{
            description: "Google Drive where your file will be saved",
            label: "Select Drive to save file",
            name: "drive",
            options: [{ name: "mydrive", label: "My Drive" }, { name: "fake_drive", label: "fake_drive_label" }],
            default: "mydrive",
            interactive: true,
            required: true,
            type: "select",
          }, {
            description: "Enter the full Google Drive URL of the folder where you want to save your data. It should look something like https://drive.google.com/corp/drive/folders/xyz. If this is inaccessible, your data will be saved to the root folder of your Google Drive. You do not need to enter a URL if you have already chosen a folder in the dropdown menu.\n",
            label: "Google Drive Destination URL",
            name: "folderid",
            type: "string",
            required: false,
          }, {
            description: "Fetch folders",
            name: "fetchpls",
            type: "select",
            interactive: true,
            label: "Select Fetch to fetch a list of folders in this drive",
            options: [{ label: "Fetch", name: "fetch" }],
          }, {
            label: "Enter a filename",
            name: "filename",
            type: "string",
            required: true,
          }],
          state: {
            data: JSON.stringify({ tokens: "access", redirect: "url" }),
          },
        }).and.notify(stubClient.restore).and.notify(done)
      })
    })

    describe("filename handling", () => {
      it("will sanitize apostrophe in filename", () => {
        const filename = "Barbara'sFile.doc"
        const sanitizedName = action.sanitizeFilename(filename)
        chai.expect(sanitizedName).to.equal("Barbara\\'sFile.doc")

        const request = new Hub.ActionRequest()
        const dataBuffer = Buffer.from("col1,col2\nval1,val2")
        request.type = Hub.ActionType.Query
        request.attachment = { dataBuffer, fileExtension: "csv" }
        request.formParams = { filename: filename, folder: stubFolder }
        request.params = {
          state_url: "https://looker.state.url.com/action_hub_state/asdfasdfasdfasdf",
          state_json: JSON.stringify({ tokens: "access", redirect: "url" }),
        }

        const stubDriveClient = sinon.stub(action as any, "driveClientFromRequest")
          .resolves({
            files: {
              create: async (params: any) => {
                chai.expect(params.requestBody.name).to.equal(sanitizedName)
                return { data: { id: stubDocId } }
              },
            },
          })

        const stubDocsClient = sinon.stub(action as any, "docsClientFromRequest")
          .resolves({
            documents: {
              batchUpdate: async () => Promise.resolve({}),
            },
          })

        return chai.expect(action.validateAndExecute(request)).to.eventually.be.fulfilled.then(() => {
          stubDriveClient.restore()
          stubDocsClient.restore()
        })
      })
    })
  })
})

class TestActionRequest extends Hub.ActionRequest {
  suggestedFilename() {
    return null
  }
} 
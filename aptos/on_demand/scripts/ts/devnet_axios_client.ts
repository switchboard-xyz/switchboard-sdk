/* eslint-disable no-console */

/**
 * Example to demonstrate how one can config the SDK to use a custom client.
 *
 * The SDK by default supports axios client on web environment and go client on node environment
 * with http2 support.
 *
 * Need to provide a function with the signature
 * `<Req, Res>(requestOptions: ClientRequest<Req>): Promise<ClientResponse<Res>>;`
 *
 */
import {
  Aptos,
  AptosConfig,
  ClientResponse,
  ClientRequest,
  Network,
  NetworkToNetworkName,
} from "@aptos-labs/ts-sdk";
import axios, { AxiosRequestConfig } from "axios";

// Default to devnet, but allow for overriding
const APTOS_NETWORK: Network =
  NetworkToNetworkName[process.env.APTOS_NETWORK ?? Network.DEVNET];

export async function axiosCustomClient<Req, Res>(
  requestOptions: ClientRequest<Req>
): Promise<ClientResponse<Res>> {
  const { params, method, url, headers, body } = requestOptions;

  const customHeaders: any = {
    ...headers,
    customClient: true,
  };

  const config: AxiosRequestConfig = {
    url,
    method,
    headers: customHeaders,
    data: body,
    params,
  };

  try {
    const response = await axios(config);
    return {
      status: response.status,
      statusText: response.statusText,
      data: response.data,
      headers: response.headers,
      config: response.config,
      request: response.request,
    };
  } catch (error: any) {
    console.error("Error making request:", error.message);
    throw error;
  }
}

const example = async () => {
  console.log(
    "This example demonstrates how one can configure a custom client to be used by the SDK"
  );

  async function withAxiosClient() {
    const config = new AptosConfig({
      network: APTOS_NETWORK,
      client: { provider: axiosCustomClient },
    });
    const aptos = new Aptos(config);

    console.log(`\nclient being used ${config.client.provider.name}`);

    const chainInfo = await aptos.getLedgerInfo();
    console.log(`${JSON.stringify(chainInfo)}`);

    const latestVersion = await aptos.getIndexerLastSuccessVersion();
    console.log(`Latest indexer version: ${latestVersion}`);

    const result = await aptos.getAccountResources({
      accountAddress: "0x1",
    });

    console.log(result);
  }

  // Call the function with axios as client
  await withAxiosClient();
};

example();

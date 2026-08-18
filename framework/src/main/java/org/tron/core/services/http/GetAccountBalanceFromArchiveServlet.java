package org.tron.core.services.http;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.tron.core.Wallet;
import org.tron.protos.contract.BalanceContract;

/** Experimental single-purpose endpoint for archive-versus-trace balance validation. */
@Component
@Slf4j(topic = "API")
public class GetAccountBalanceFromArchiveServlet extends RateLimiterServlet {

  @Autowired
  private Wallet wallet;

  protected void doPost(HttpServletRequest request, HttpServletResponse response) {
    try {
      PostParams params = PostParams.getPostParams(request);
      BalanceContract.AccountBalanceRequest.Builder builder =
          BalanceContract.AccountBalanceRequest.newBuilder();
      JsonFormat.merge(params.getParams(), builder, params.isVisible());
      BalanceContract.AccountBalanceResponse reply =
          wallet.getAccountBalanceFromStateArchive(builder.build());
      response.getWriter().println(JsonFormat.printToString(reply, params.isVisible()));
    } catch (Exception failure) {
      Util.processError(failure, response);
    }
  }
}

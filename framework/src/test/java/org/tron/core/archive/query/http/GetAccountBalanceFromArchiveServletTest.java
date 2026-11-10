package org.tron.core.archive.query.http;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.AnnotatedBeanDefinitionReader;
import org.springframework.test.util.ReflectionTestUtils;
import org.tron.core.Wallet;
import org.tron.core.config.args.Args;
import org.tron.core.services.http.JsonFormat;
import org.tron.core.services.http.Util;
import org.tron.protos.contract.BalanceContract.AccountBalanceRequest;
import org.tron.protos.contract.BalanceContract.AccountBalanceResponse;
import org.tron.protos.contract.BalanceContract.AccountIdentifier;
import org.tron.protos.contract.BalanceContract.BlockBalanceTrace.BlockIdentifier;

public class GetAccountBalanceFromArchiveServletTest {

  private long previousMaxMessageSize;

  @Before
  public void configureHttpLimit() {
    previousMaxMessageSize = Args.getInstance().getHttpMaxMessageSize();
    Args.getInstance().setHttpMaxMessageSize(1L << 20);
  }

  @After
  public void restoreHttpLimit() {
    Args.getInstance().setHttpMaxMessageSize(previousMaxMessageSize);
  }

  @Test
  public void preservesRequestDecodingAndResponseEncoding() throws Exception {
    byte[] address = new byte[21];
    address[0] = 0x41;
    AccountBalanceRequest input = AccountBalanceRequest.newBuilder()
        .setAccountIdentifier(AccountIdentifier.newBuilder()
            .setAddress(ByteString.copyFrom(address)))
        .setBlockIdentifier(BlockIdentifier.newBuilder().setNumber(12)).build();
    AccountBalanceResponse reply = AccountBalanceResponse.newBuilder().setBalance(123).build();
    Wallet wallet = mock(Wallet.class);
    when(wallet.getAccountBalanceFromStateArchive(input)).thenReturn(reply);
    GetAccountBalanceFromArchiveServlet servlet = new GetAccountBalanceFromArchiveServlet();
    ReflectionTestUtils.setField(servlet, "wallet", wallet);
    HttpServletRequest request = request(JsonFormat.printToString(input, false));
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter output = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(output));

    servlet.doPost(request, response);

    verify(wallet).getAccountBalanceFromStateArchive(input);
    assertEquals(JsonFormat.printToString(reply, false), output.toString().trim());
  }

  @Test
  public void preservesWalletFailureWireShape() throws Exception {
    Wallet wallet = mock(Wallet.class);
    IllegalStateException failure = new IllegalStateException("Archive unavailable");
    when(wallet.getAccountBalanceFromStateArchive(AccountBalanceRequest.getDefaultInstance()))
        .thenThrow(failure);
    GetAccountBalanceFromArchiveServlet servlet = new GetAccountBalanceFromArchiveServlet();
    ReflectionTestUtils.setField(servlet, "wallet", wallet);
    HttpServletResponse response = mock(HttpServletResponse.class);
    StringWriter output = new StringWriter();
    when(response.getWriter()).thenReturn(new PrintWriter(output));

    servlet.doPost(request("{}"), response);

    verify(wallet).getAccountBalanceFromStateArchive(AccountBalanceRequest.getDefaultInstance());
    assertEquals(Util.printErrorMsg(failure), output.toString().trim());
  }

  @Test
  public void preservesSpringBeanNameAndRateLimiterKey() {
    DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
    new AnnotatedBeanDefinitionReader(beans).register(GetAccountBalanceFromArchiveServlet.class);

    assertEquals(GetAccountBalanceFromArchiveServlet.class.getName(),
        beans.getBeanDefinition("getAccountBalanceFromArchiveServlet").getBeanClassName());
    assertEquals("GetAccountBalanceFromArchiveServlet",
        GetAccountBalanceFromArchiveServlet.class.getSimpleName());
  }

  private static HttpServletRequest request(String body) throws Exception {
    HttpServletRequest request = mock(HttpServletRequest.class);
    when(request.getContentType()).thenReturn("application/json");
    when(request.getReader()).thenReturn(new BufferedReader(new StringReader(body)));
    return request;
  }
}

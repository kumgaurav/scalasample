package com.kumargaurav
import com.google.auth.oauth2.GoogleCredentials
import com.google.auth.oauth2.ImpersonatedCredentials
import com.google.auth.oauth2.AccessToken
import com.google.auth.oauth2.ServiceAccountCredentials

object MiscUtil {
  def getAccessToken(defaultServiceAccount: String): AccessToken = {
    val sourceCredentials = GoogleCredentials.getApplicationDefault
    val scopes = java.util.Arrays.asList("https://www.googleapis.com/auth/devstorage.read_only", "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/bigquery")
    val impersonatedCredentials = ImpersonatedCredentials.newBuilder()
      .setSourceCredentials(sourceCredentials)
      .setTargetPrincipal(defaultServiceAccount)
      .setScopes(scopes)
      .setLifetime(3600)
      .build()
    impersonatedCredentials.refreshAccessToken()
  }
}

param(
  [Parameter(Mandatory = $true)]
  [ValidateSet('set','verify','rollback')]
  [string]$Action,

  [string]$BotToken = $env:BOT_TOKEN,
  [string]$WebhookSecret = $env:BOT_WEBHOOK_SECRET,
  [string]$BaseUrl = $env:SEARCH_BOT_WEBHOOK_BASE_URL
)

$ErrorActionPreference = 'Stop'

function Assert-Required([string]$name, [string]$value) {
  if ([string]::IsNullOrWhiteSpace($value)) {
    throw "Missing required value: $name"
  }
}

function Build-WebhookUrl([string]$base, [string]$secret) {
  $trimmed = $base.TrimEnd('/')
  return "$trimmed/telegram/webhook/$secret"
}

function Tg-Post([string]$token, [string]$method, [hashtable]$body) {
  $uri = "https://api.telegram.org/bot$token/$method"
  return Invoke-RestMethod -Method Post -Uri $uri -Body $body
}

function Tg-Get([string]$token, [string]$method) {
  $uri = "https://api.telegram.org/bot$token/$method"
  return Invoke-RestMethod -Method Get -Uri $uri
}

switch ($Action) {
  'set' {
    Assert-Required 'BOT_TOKEN' $BotToken
    Assert-Required 'BOT_WEBHOOK_SECRET' $WebhookSecret
    Assert-Required 'SEARCH_BOT_WEBHOOK_BASE_URL' $BaseUrl

    $webhookUrl = Build-WebhookUrl $BaseUrl $WebhookSecret
    Write-Host "[set] setting webhook to: $webhookUrl" -ForegroundColor Cyan

    $resp = Tg-Post $BotToken 'setWebhook' @{ url = $webhookUrl }
    $resp | ConvertTo-Json -Depth 8
    break
  }

  'verify' {
    Assert-Required 'BOT_TOKEN' $BotToken

    $info = Tg-Get $BotToken 'getWebhookInfo'
    $currentUrl = $info.result.url
    Write-Host "[verify] current webhook: $currentUrl" -ForegroundColor Cyan

    if (-not [string]::IsNullOrWhiteSpace($WebhookSecret) -and -not [string]::IsNullOrWhiteSpace($BaseUrl)) {
      $expected = Build-WebhookUrl $BaseUrl $WebhookSecret
      Write-Host "[verify] expected: $expected" -ForegroundColor DarkGray
      if ($currentUrl -eq $expected) {
        Write-Host '[verify] matched' -ForegroundColor Green
      } else {
        Write-Host '[verify] not matched' -ForegroundColor Yellow
      }
    }

    $info | ConvertTo-Json -Depth 8
    break
  }

  'rollback' {
    Assert-Required 'BOT_TOKEN' $BotToken

    Write-Host '[rollback] deleting webhook...' -ForegroundColor Cyan
    $resp = Tg-Post $BotToken 'deleteWebhook' @{}
    $resp | ConvertTo-Json -Depth 8

    $info = Tg-Get $BotToken 'getWebhookInfo'
    $currentUrl = $info.result.url
    if ([string]::IsNullOrWhiteSpace($currentUrl)) {
      Write-Host '[rollback] success: webhook cleared' -ForegroundColor Green
    } else {
      Write-Host "[rollback] warning: webhook still present: $currentUrl" -ForegroundColor Yellow
    }
    break
  }
}

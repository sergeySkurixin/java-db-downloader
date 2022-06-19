Program for selecting from DB with huge searching params

For example, find userIds for 800k phones

<table>
Sync(1thread)
<tr>
<td><b>BatchSize</b></td>
<td><b>Clients per second</b></td>
<td><b>Summary downloading time</b></td>
</tr>

<tr>
<td>5</td>
<td>10</td>
<td></td>
</tr>

<tr>
<td>50</td>
<td>155</td>
<td></td>
</tr>

<tr>
<td>200</td>
<td>188</td>
<td></td>
</tr>

<tr>
<td>500</td>
<td>170</td>
<td></td>
</tr>
</table>

<table>
Async downloader
<tr>
<td><b>Threads</b></td>
<td><b>BatchSize</b></td>
<td><b>Clients per second</b></td>
<td><b>Summary downloading time</b></td>
</tr>

<tr>
<td>10</td>
<td>100</td>
<td>1800</td>
<td></td>
</tr>
</table>
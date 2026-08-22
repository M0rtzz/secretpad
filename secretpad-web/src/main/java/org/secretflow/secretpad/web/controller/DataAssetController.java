package org.secretflow.secretpad.web.controller;

import org.secretflow.secretpad.service.model.common.SecretPadResponse;
import org.secretflow.secretpad.web.service.DataAssetService;
import org.secretflow.secretpad.web.service.DatabaseAssetImportService;
import org.secretflow.secretpad.web.service.SandboxDataControlService;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.net.*;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

@RestController
@RequestMapping("/api/v1alpha1/data-assets")
public class DataAssetController {
    private final DataAssetService service;
    private final DatabaseAssetImportService databaseImport;
    private final SandboxDataControlService dataControl;
    public DataAssetController(DataAssetService service, DatabaseAssetImportService databaseImport, SandboxDataControlService dataControl){this.service=service;this.databaseImport=databaseImport;this.dataControl=dataControl;}
    @PostMapping(value="/files/upload", consumes="multipart/form-data")
    public SecretPadResponse<Map<String,Object>> upload(@RequestPart("file") MultipartFile file) throws Exception {
        String type=file.getContentType()==null?"":file.getContentType().toLowerCase(Locale.ROOT);
        boolean png="image/png".equals(type); boolean csv="text/csv".equals(type)||file.getOriginalFilename()!=null&&file.getOriginalFilename().toLowerCase(Locale.ROOT).endsWith(".csv");
        long max=png?20L*1024*1024:500L*1024*1024;
        if((!png&&!csv)||file.getSize()>max) throw new IllegalArgumentException("仅支持 CSV(500MB) 或 PNG(20MB)");
        Path temp=Files.createTempFile("secretpad-asset-",png?".png":".csv");
        try {
            try(InputStream in=file.getInputStream();OutputStream out=Files.newOutputStream(temp)){in.transferTo(out);}
            byte[] signature=new byte[8]; MessageDigest digest=MessageDigest.getInstance("SHA-256");
            try(InputStream in=Files.newInputStream(temp)){int offset=0,n;byte[] buf=new byte[64*1024];while((n=in.read(buf))!=-1){if(offset<8){int c=Math.min(n,8-offset);System.arraycopy(buf,0,signature,offset,c);offset+=c;}digest.update(buf,0,n);}}
            if(png&&(signature[0]!=(byte)0x89||signature[1]!=0x50||signature[2]!=0x4e||signature[3]!=0x47)) throw new IllegalArgumentException("PNG 文件签名无效");
            String checksum=HexFormat.of().formatHex(digest.digest());
            String key="uploads/"+UUID.randomUUID()+"/"+(file.getOriginalFilename()==null?"asset":file.getOriginalFilename());
            String uri=service.storage().put(key,temp.toFile(),png?"image/png":"text/csv",checksum);
            return SecretPadResponse.success(service.registerUpload(file.getOriginalFilename(),png?"image/png":"text/csv","RAW",uri,checksum,file.getSize()));
        } finally {
            Files.deleteIfExists(temp);
        }
    }
    @PostMapping("/api-snapshots")
    public SecretPadResponse<Map<String,Object>> apiSnapshot(@RequestBody Map<String,Object> request) throws Exception {
        String url=String.valueOf(request.getOrDefault("url", "")).trim();
        URI source=URI.create(url);
        if(!Set.of("http","https").contains(source.getScheme())||source.getHost()==null) throw new IllegalArgumentException("仅支持 HTTP/HTTPS GET 接口");
        HttpRequest.Builder builder=HttpRequest.newBuilder(source).timeout(Duration.ofSeconds(60)).GET();
        Object headerValue=request.get("headers");
        if(headerValue instanceof Map<?,?> headers) headers.forEach((k,v)->builder.header(String.valueOf(k),String.valueOf(v)));
        HttpResponse<InputStream> response=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).followRedirects(HttpClient.Redirect.NEVER).build().send(builder.build(),HttpResponse.BodyHandlers.ofInputStream());
        if(response.statusCode()<200||response.statusCode()>=300) { response.body().close(); throw new IllegalArgumentException("API 返回 HTTP "+response.statusCode()); }
        String responseType=response.headers().firstValue("content-type").orElse("text/csv").split(";")[0].toLowerCase(Locale.ROOT);
        boolean png="image/png".equals(responseType); boolean csv="text/csv".equals(responseType)||"application/csv".equals(responseType)||"application/octet-stream".equals(responseType);
        if(!png&&!csv) { response.body().close(); throw new IllegalArgumentException("API 快照仅支持 CSV 或 PNG 响应"); }
        long max=png?20L*1024*1024:500L*1024*1024; Path temp=Files.createTempFile("secretpad-api-snapshot-",png?".png":".csv");
        try {
            MessageDigest digest=MessageDigest.getInstance("SHA-256"); long size=0; byte[] signature=new byte[8]; int signatureOffset=0;
            try(InputStream in=response.body();OutputStream fileOut=Files.newOutputStream(temp);DigestOutputStream out=new DigestOutputStream(fileOut,digest)){byte[] buf=new byte[64*1024];int n;while((n=in.read(buf))!=-1){size+=n;if(size>max)throw new IllegalArgumentException("API 快照超过大小限制");if(signatureOffset<8){int c=Math.min(n,8-signatureOffset);System.arraycopy(buf,0,signature,signatureOffset,c);signatureOffset+=c;}out.write(buf,0,n);}}
            if(png&&(signature[0]!=(byte)0x89||signature[1]!=0x50||signature[2]!=0x4e||signature[3]!=0x47))throw new IllegalArgumentException("PNG 文件签名无效");
            String checksum=HexFormat.of().formatHex(digest.digest()); String name=String.valueOf(request.getOrDefault("name", source.getHost()+"-snapshot"));
            String key="api-snapshots/"+UUID.randomUUID()+"/"+name+(png?".png":".csv"); String uri=service.storage().put(key,temp.toFile(),png?"image/png":"text/csv",checksum);
            return SecretPadResponse.success(service.registerStored(name,png?"image/png":"text/csv","RAW",uri,checksum,size,"API_SNAPSHOT"));
        } finally { Files.deleteIfExists(temp); }
    }
    @GetMapping("/catalog") public SecretPadResponse<List<Map<String,Object>>> catalog(@RequestParam(defaultValue="") String keyword){return SecretPadResponse.success(service.catalog(keyword));}
    @GetMapping("/detail") public SecretPadResponse<Map<String,Object>> detail(@RequestParam String id){return SecretPadResponse.success(service.detail(id));}
    @GetMapping("/projects/catalog") public SecretPadResponse<List<Map<String,Object>>> projectAssets(@RequestParam String projectId){return SecretPadResponse.success(service.projectAssets(projectId));}
    @PostMapping("/projects/attach") public SecretPadResponse<List<Map<String,Object>>> attachProjectAssets(@RequestBody Map<String,Object> r){return SecretPadResponse.success(service.attachProjectAssets(r));}
    @GetMapping("/sandboxes/mounts") public SecretPadResponse<List<Map<String,Object>>> sandboxMounts(@RequestParam String sandboxId){return SecretPadResponse.success(service.sandboxMounts(sandboxId));}
    @GetMapping("/preview") public SecretPadResponse<Map<String,Object>> preview(@RequestParam String id,@RequestParam(defaultValue="5") int limit){return SecretPadResponse.success(service.preview(id,limit));}
    @GetMapping("/content") public ResponseEntity<byte[]> imageContent(@RequestParam String id){
        DataAssetService.ImageContent image=service.previewImage(id);
        MediaType type=MediaType.parseMediaType(image.contentType());
        return ResponseEntity.ok().header(HttpHeaders.CACHE_CONTROL,"private, max-age=300").contentType(type).body(image.content());
    }
    @PostMapping("/database/preview") public SecretPadResponse<Map<String,Object>> databasePreview(@RequestBody Map<String,Object> request){return SecretPadResponse.success(databaseImport.preview(request));}
    @PostMapping("/database/test") public SecretPadResponse<Map<String,Object>> databaseTest(@RequestBody Map<String,Object> request){return SecretPadResponse.success(databaseImport.testConnection(request));}
    @PostMapping("/database/import") public SecretPadResponse<Map<String,Object>> databaseImport(@RequestBody Map<String,Object> request){return SecretPadResponse.success(databaseImport.importAsset(request));}
    @PostMapping("/delete") public SecretPadResponse<Map<String,Object>> delete(@RequestBody Map<String,Object> r){return SecretPadResponse.success(service.delete(String.valueOf(r.get("id"))));}
    @GetMapping("/usage-controls/requests") public SecretPadResponse<List<Map<String,Object>>> requests(){return SecretPadResponse.success(service.usageRequests());}
    @PostMapping("/usage-controls/save") public SecretPadResponse<Map<String,Object>> save(@RequestBody Map<String,Object> r){return SecretPadResponse.success(service.saveUsage(r));}
    @PostMapping("/usage-controls/review") public SecretPadResponse<Map<String,Object>> review(@RequestBody Map<String,Object> r){return SecretPadResponse.success(service.reviewUsage(r));}
    @GetMapping("/usage-controls/mounts") public SecretPadResponse<List<Map<String,Object>>> mountControls(){return SecretPadResponse.success(dataControl.mountControls());}
    @PostMapping("/usage-controls/mounts/save") public SecretPadResponse<Map<String,Object>> saveMountControl(@RequestBody Map<String,Object> r){return SecretPadResponse.success(dataControl.saveMountControl(r));}
}

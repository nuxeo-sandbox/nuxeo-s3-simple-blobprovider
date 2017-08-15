/*
 * (C) Copyright 2017 Nuxeo (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Michael Vachette
 */

package org.nuxeo.labs.s3.simple.blobprovider;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.HttpMethod;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.common.file.FileCache;
import org.nuxeo.common.file.LRUFileCache;
import org.nuxeo.common.utils.RFC2231;
import org.nuxeo.common.utils.SizeUtils;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.NuxeoException;
import org.nuxeo.ecm.core.blob.AbstractBlobProvider;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.ManagedBlob;
import org.nuxeo.ecm.core.blob.SimpleManagedBlob;
import org.nuxeo.ecm.core.io.download.DownloadHelper;
import org.nuxeo.ecm.core.model.Document;
import org.nuxeo.runtime.api.Framework;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;
import java.util.Map;

public class SimpleS3Blobprovider extends AbstractBlobProvider {

    private static final Log log = LogFactory.getLog(SimpleS3Blobprovider.class);

    public static final String BUCKET_NAME_PROPERTY = "bucket";

    public static final String BUCKET_REGION_PROPERTY = "region";

    public static final String AWS_ID_PROPERTY = "awsid";

    public static final String AWS_SECRET_PROPERTY = "awssecret";

    public static final String DIRECTDOWNLOAD_PROPERTY = "directdownload";

    public static final String DIRECTDOWNLOAD_EXPIRE_PROPERTY = "directdownload.expire";

    public static final String CACHE_SIZE_PROPERTY = "cachesize";

    public static final String CACHE_COUNT_PROPERTY = "cachecount";

    public static final String CACHE_MIN_AGE_PROPERTY = "cacheminage";

    String bucketName;

    boolean directDownload;

    int directDownloadExpire;

    AmazonS3 amazonS3;

    protected File cachedir;

    public FileCache fileCache;


    @Override
    public void initialize(String blobProviderId, Map<String, String> properties) throws IOException {
        super.initialize(blobProviderId, properties);
        bucketName = properties.get(BUCKET_NAME_PROPERTY);
        String bucketRegion = properties.get(BUCKET_REGION_PROPERTY);
        String awsID = properties.get(AWS_ID_PROPERTY);
        String awsSecret = properties.get(AWS_SECRET_PROPERTY);

        String cacheSizeStr = properties.getOrDefault(CACHE_SIZE_PROPERTY, "100 mb");
        String cacheCountStr = properties.getOrDefault(CACHE_COUNT_PROPERTY, "10000");
        String minAgeStr = properties.getOrDefault(CACHE_MIN_AGE_PROPERTY, "3600");

        directDownload = Boolean.parseBoolean(properties.getOrDefault(DIRECTDOWNLOAD_PROPERTY, "false"));
        directDownloadExpire = Integer.parseInt(properties.getOrDefault(DIRECTDOWNLOAD_EXPIRE_PROPERTY,"1"));

        amazonS3 = new AmazonS3Client(new BasicAWSCredentials(awsID,awsSecret),new ClientConfiguration());
        amazonS3.setRegion(Region.getRegion(Regions.fromName(bucketRegion)));
        initializeCache(SizeUtils.parseSizeInBytes(cacheSizeStr),Long.parseLong(cacheCountStr),Long.parseLong(minAgeStr));

    }

    @Override
    public Blob readBlob(BlobManager.BlobInfo blobInfo) throws IOException {
        if (blobInfo == null || blobInfo.key == null) {
            throw new IOException("Invalid blobinfo: "+blobInfo);
        }
        return new SimpleManagedBlob(blobInfo);
    }

    @Override
    public String writeBlob(Blob blob, Document doc) throws IOException {
        throw new UnsupportedOperationException("Write not supported");
    }

    @Override
    public InputStream getStream(ManagedBlob blob) throws IOException {
        File cachedFile = getFileFromCache(blob);
        return new FileInputStream(cachedFile);
    }

    @Override
    public boolean supportsUserUpdate() {
        return false;
    }

    public AmazonS3 getClient() {
        return amazonS3;
    }

    public String getBucketName() {
        return bucketName;
    }

    public File getFileFromCache(ManagedBlob blob) throws IOException {
        String key = blob.getKey().split(":")[1];
        S3Object object;
        try {
            GetObjectRequest request = new GetObjectRequest(getBucketName(),key);
            object = getClient().getObject(request);
        } catch (AmazonS3Exception e) {
            throw new IOException(String.format("Could get key %s in bucket %s",key,bucketName));
        }
        String etag = object.getObjectMetadata().getETag();
        File cachedFile = fileCache.getFile(etag);
        if (cachedFile == null) {
            File tmp = fileCache.getTempFile();
            FileUtils.copyInputStreamToFile(object.getObjectContent(),tmp);
            fileCache.putFile(etag, tmp);
            cachedFile = fileCache.getFile(etag);
        }
        object.close();
        return cachedFile;
    }


    protected boolean isDirectDownload() {
        return directDownload;
    }

    @Override
    public URI getURI(ManagedBlob blob, BlobManager.UsageHint hint, HttpServletRequest servletRequest)
            throws IOException {
        if (hint != BlobManager.UsageHint.DOWNLOAD || !isDirectDownload()) {
            return null;
        }
        String digest = blob.getKey();
        // strip prefix
        int colon = digest.indexOf(':');
        if (colon >= 0) {
            digest = digest.substring(colon + 1);
        }

        return getRemoteUri(digest, blob, servletRequest);
    }


    protected URI getRemoteUri(String digest, ManagedBlob blob, HttpServletRequest servletRequest) throws IOException {
        String key = digest;
        Date expiration = new Date();
        expiration.setTime(expiration.getTime() + directDownloadExpire * 1000);
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, key, HttpMethod.GET);
        request.addRequestParameter("response-content-type", getContentTypeHeader(blob));
        request.addRequestParameter("response-content-disposition", getContentDispositionHeader(blob, null));
        request.setExpiration(expiration);
        URL url = amazonS3.generatePresignedUrl(request);
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    protected String getContentTypeHeader(Blob blob) {
        String contentType = blob.getMimeType();
        String encoding = blob.getEncoding();
        if (contentType != null && !StringUtils.isBlank(encoding)) {
            int i = contentType.indexOf(';');
            if (i >= 0) {
                contentType = contentType.substring(0, i);
            }
            contentType += "; charset=" + encoding;
        }
        return contentType;
    }

    protected String getContentDispositionHeader(Blob blob, HttpServletRequest servletRequest) {
        if (servletRequest == null) {
            return RFC2231.encodeContentDisposition(blob.getFilename(), false, null);
        } else {
            return DownloadHelper.getRFC2231ContentDisposition(servletRequest, blob.getFilename());
        }
    }

    /**
     * Initialize the cache.
     *
     * @param maxSize the maximum size of the cache (in bytes)
     * @param maxCount the maximum number of files in the cache
     * @param minAge the minimum age of a file in the cache to be eligible for removal (in seconds)
     * @since 5.9.2
     */
    protected void initializeCache(long maxSize, long maxCount, long minAge) throws IOException {
        cachedir = Framework.createTempFile("nxbincache.", "");
        cachedir.delete();
        cachedir.mkdir();
        fileCache = new LRUFileCache(cachedir, maxSize, maxCount, minAge);
    }

    @Override
    public void close() {
        fileCache.clear();
        if (cachedir != null) {
            try {
                FileUtils.deleteDirectory(cachedir);
            } catch (IOException e) {
                throw new NuxeoException(e);
            }
        }
    }
}

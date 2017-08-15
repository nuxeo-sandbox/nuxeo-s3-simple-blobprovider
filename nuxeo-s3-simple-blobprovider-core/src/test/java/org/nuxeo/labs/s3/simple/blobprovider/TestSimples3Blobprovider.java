/*
 * (C) Copyright 2015-2017 Nuxeo (http://nuxeo.com/) and others.
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

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.nuxeo.ecm.automation.OperationException;
import org.nuxeo.ecm.core.api.Blob;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.blob.BlobManager;
import org.nuxeo.ecm.core.blob.BlobProvider;
import org.nuxeo.ecm.core.blob.ManagedBlob;
import org.nuxeo.ecm.core.test.DefaultRepositoryInit;
import org.nuxeo.ecm.core.test.annotations.Granularity;
import org.nuxeo.ecm.core.test.annotations.RepositoryConfig;
import org.nuxeo.ecm.platform.test.PlatformFeature;
import org.nuxeo.runtime.api.Framework;
import org.nuxeo.runtime.test.runner.Deploy;
import org.nuxeo.runtime.test.runner.FeaturesRunner;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

@RunWith(FeaturesRunner.class)
@org.nuxeo.runtime.test.runner.Features(PlatformFeature.class)
@RepositoryConfig(init = DefaultRepositoryInit.class, cleanup = Granularity.METHOD)
@Deploy({ "nuxeo-s3-simple-blobprovider-core" })
public class TestSimples3Blobprovider {

    @Inject
    CoreSession session;

    @Inject
    BlobManager blobManager;

    @Test
    public void testGetBlob() throws IOException, OperationException {
        BlobProvider s3provider = new SimpleS3Blobprovider();
        Map<String,String> properties = new HashMap<>();
        properties.put("region",System.getProperty("simple.s3.region"));
        properties.put("bucket",System.getProperty("simple.s3.bucket"));
        properties.put("awsid",System.getProperty("simple.s3.awsid"));
        properties.put("awssecret",System.getProperty("simple.s3.awssecret"));
        s3provider.initialize("s3simple",properties);
        blobManager.getBlobProviders().put("s3simple",s3provider);
        BlobManager.BlobInfo info = new BlobManager.BlobInfo();
        info.key = "s3simple:"+"CB_102_55A-3_55A-1.mov";
        info.filename = "CB_102_55A-3_55A-1.mov";
        Blob blob = s3provider.readBlob(info);
        File tmp = Framework.createTempFile("nxtmp-", "");
        FileUtils.copyInputStreamToFile(blob.getStream(),tmp);

        Assert.assertTrue(tmp.length()==4074170);
    }

    @Test
    public void testGetDirectDownload() throws IOException, OperationException {
        BlobProvider s3provider = new SimpleS3Blobprovider();
        Map<String,String> properties = new HashMap<>();
        properties.put("region",System.getProperty("simple.s3.region"));
        properties.put("bucket",System.getProperty("simple.s3.bucket"));
        properties.put("awsid",System.getProperty("simple.s3.awsid"));
        properties.put("awssecret",System.getProperty("simple.s3.awssecret"));
        properties.put("directdownload","true");
        s3provider.initialize("s3simple",properties);
        blobManager.getBlobProviders().put("s3simple",s3provider);
        BlobManager.BlobInfo info = new BlobManager.BlobInfo();
        info.key = "s3simple:"+"CB_102_55A-3_55A-1.mov";
        info.filename = "CB_102_55A-3_55A-1.mov";
        ManagedBlob blob = (ManagedBlob) s3provider.readBlob(info);
        URI downloadURI = s3provider.getURI(blob, BlobManager.UsageHint.DOWNLOAD,null);
        Assert.assertNotNull(downloadURI);
    }

    @Test(expected = IOException.class)
    public void testGetBlobNull() throws IOException, OperationException {
        BlobProvider s3provider = new SimpleS3Blobprovider();
        Map<String,String> properties = new HashMap<>();
        properties.put("region",System.getProperty("simple.s3.region"));
        properties.put("bucket",System.getProperty("simple.s3.bucket"));
        properties.put("awsid",System.getProperty("simple.s3.awsid"));
        properties.put("awssecret",System.getProperty("simple.s3.awssecret"));
        s3provider.initialize("s3simple",properties);
        blobManager.getBlobProviders().put("s3simple",s3provider);
        Blob blob = s3provider.readBlob(null);
    }

}

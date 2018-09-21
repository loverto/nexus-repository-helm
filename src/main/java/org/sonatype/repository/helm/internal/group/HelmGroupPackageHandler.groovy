package org.sonatype.repository.helm.internal.group

import org.joda.time.DateTime
import org.sonatype.nexus.common.collect.NestedAttributesMap
import org.sonatype.nexus.repository.Repository
import org.sonatype.nexus.repository.group.GroupFacet
import org.sonatype.nexus.repository.group.GroupHandler
import org.sonatype.nexus.repository.http.HttpResponses
import org.sonatype.nexus.repository.http.HttpStatus
import org.sonatype.nexus.repository.view.Content
import org.sonatype.nexus.repository.view.ContentTypes
import org.sonatype.nexus.repository.view.Context
import org.sonatype.nexus.repository.view.Response
import org.sonatype.nexus.repository.view.matchers.token.TokenMatcher
import org.sonatype.nexus.repository.view.payloads.BytesPayload

import javax.annotation.Nonnull
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

import static org.sonatype.nexus.repository.http.HttpConditions.makeConditional
import static org.sonatype.nexus.repository.http.HttpConditions.makeUnconditional

@Named
@Singleton
class HelmGroupPackageHandler extends GroupHandler {
    private final boolean mergeMetadata

    @Inject
    HelmGroupPackageHandler(@Named('nexus.helm.mergeGroupMetadata:-true') final boolean mergeMetadata){
        this.mergeMetadata=mergeMetadata
    }
    @Override
    protected Response doGet(@Nonnull final Context context,
                             @Nonnull final GroupHandler.DispatchedRepositories dispatched)
            throws Exception
    {
        TokenMatcher.State state = context.attributes.require(TokenMatcher.State)
        Repository repository = context.repository
        GroupFacet groupFacet = repository.facet(GroupFacet)

        log.debug '[getPackage] group repository: {} tokens: {}', repository.name, state.tokens

        // Remove conditional headers before making "internal" requests: https://issues.sonatype.org/browse/NEXUS-13915
        makeUnconditional(context.getRequest())

        LinkedHashMap<Repository, Response> responses = null
        try {
            // get all and filter for HTTP OK responses
            responses = getAll(context, groupFacet.members(), dispatched)
                    .findAll { k, v -> v.status.code == HttpStatus.OK }
        }
        finally {
            makeConditional(context.getRequest())
        }

        if (responses == null || responses.isEmpty()) {
            return HttpResponses.notFound();
        }

        // unroll the actual package metadata from content attributes
        final List<NestedAttributesMap> packages = responses
                .collect { k, v -> ((Content) v.payload).attributes.get(NestedAttributesMap) }

        def result
        if (shouldServeFirstResult(packages, NpmHandlers.packageId(state))) {
            result = packages[0]
        }
        else {
            log.debug("Merging results from {} repositories", responses.size())
            result = NpmMetadataUtils.merge(packages[0].key, packages.reverse())
        }

        NpmMetadataUtils.rewriteTarballUrl(repository.name, result)

        Content content = new Content(new BytesPayload(NpmJsonUtils.bytes(result), ContentTypes.APPLICATION_JSON))
        content.attributes.set(Content.CONTENT_LAST_MODIFIED, DateTime.now())
        content.attributes.set(NestedAttributesMap, result)
        return HttpResponses.ok()
    }


}

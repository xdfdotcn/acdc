package cn.xdf.acdc.devops.service.entity.impl;

import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import cn.xdf.acdc.devops.repository.AuthorityRepository;
import cn.xdf.acdc.devops.service.entity.AuthorityService;
import io.jsonwebtoken.lang.Assert;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class AuthorityServiceImpl implements AuthorityService {

    @Autowired
    private AuthorityRepository authorityRepository;

    @Override
    public AuthorityDO save(final AuthorityDO authority) {
        Assert.notNull(authority);
        return authorityRepository.save(authority);
    }

    @Override
    public Optional<AuthorityDO> findByName(final String name) {
        Assert.hasText(name);
        return authorityRepository.findById(name);
    }

    @Override
    public List<AuthorityDO> findAll() {
        return authorityRepository.findAll();
    }
}

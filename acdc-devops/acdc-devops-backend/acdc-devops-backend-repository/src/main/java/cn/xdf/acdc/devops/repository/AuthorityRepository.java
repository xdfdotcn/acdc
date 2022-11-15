package cn.xdf.acdc.devops.repository;

import cn.xdf.acdc.devops.core.domain.entity.AuthorityDO;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data JPA repository for the {@link AuthorityDO} entity.
 */
public interface AuthorityRepository extends JpaRepository<AuthorityDO, String> {

}

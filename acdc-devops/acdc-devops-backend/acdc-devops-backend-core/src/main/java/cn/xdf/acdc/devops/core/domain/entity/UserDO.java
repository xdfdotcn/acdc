package cn.xdf.acdc.devops.core.domain.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import org.hibernate.annotations.BatchSize;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.LastModifiedBy;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.validation.constraints.Email;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Entity
@Table(name = "user")
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Accessors(chain = true)

public class UserDO extends BaseDO implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @JsonIgnore
    @Column(name = "password_hash")
    private String password;

    @Column(name = "name", length = 32)
    private String name;

    @Column(name = "domain_account", length = 1024)
    private String domainAccount;

    @Email
    @Column(name = "email", length = 254, nullable = false, unique = true)
    private String email;

    @CreatedBy
    @Column(name = "created_by", nullable = false, length = 50, updatable = false)
    @JsonIgnore
    private String createdBy;

    @LastModifiedBy
    @Column(name = "updated_By", length = 50)
    @JsonIgnore
    private String updatedBy;

    @ManyToMany
    @JoinTable(
            name = "user_authority",
            joinColumns = {@JoinColumn(name = "user_id", referencedColumnName = "id")},
            inverseJoinColumns = {@JoinColumn(name = "authority_name", referencedColumnName = "name")}
    )
    @BatchSize(size = 20)
    private Set<AuthorityDO> authorities = new HashSet<>();

    @ManyToMany(fetch = FetchType.EAGER, mappedBy = "users")
    @JsonIgnoreProperties(value = {"owner", "rdbs", "users"}, allowSetters = true)
    private Set<ProjectDO> projects = new HashSet<>();

    // functions for jpa union feature
    // CHECKSTYLE:OFF

    public void setProjects(Set<ProjectDO> projects) {
        if (this.projects != null) {
            this.projects.forEach(i -> i.removeUser(this));
        }
        if (projects != null) {
            projects.forEach(i -> i.addUser(this));
        }
        this.projects = projects;
    }

    public UserDO projects(Set<ProjectDO> projects) {
        this.setProjects(projects);
        return this;
    }

    public UserDO addProject(ProjectDO project) {
        this.projects.add(project);
        project.getUsers().add(this);
        return this;
    }

    public UserDO removeProject(ProjectDO project) {
        this.projects.remove(project);
        project.getUsers().remove(this);
        return this;
    }

    // functions for jpa union feature
    // CHECKSTYLE:ON

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof UserDO)) {
            return false;
        }
        return id != null && id.equals(((UserDO) o).id);
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    @Override
    public String toString() {
        return "User{"
                + "id=" + id
                + ", name='" + name + '\''
                + ", domainAccount='" + domainAccount + '\''
                + ", email='" + email + '\''
                + ", authorities=" + authorities
                + '}';
    }
}
